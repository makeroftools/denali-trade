// src/pipeline.rs
use flume::{self, Sender as ItemSender, Receiver as ItemReceiver};
use std::future::Future;
use std::sync::Arc;
use tokio::task::{self, JoinHandle};

#[derive(Copy, Clone)]
pub enum SendMode {
    Blocking,
    TryDrop,
}

pub struct Builder {
    input_cap: usize,
    process_cap: usize,
    output_cap: usize,
    process_workers: usize,
    filter_workers: usize,
    process_send_mode: SendMode,
    filter_send_mode: SendMode,
    log_fn: Option<Arc<dyn Fn(&str) + Send + Sync + 'static>>,
}

impl Builder {
    pub fn new() -> Self {
        Self {
            input_cap: 4096,
            process_cap: 2048,
            output_cap: 512,
            process_workers: 1,
            filter_workers: 1,
            process_send_mode: SendMode::Blocking,
            filter_send_mode: SendMode::Blocking,
            log_fn: None,
        }
    }

    pub fn input_cap(mut self, cap: usize) -> Self { self.input_cap = cap; self }
    pub fn process_cap(mut self, cap: usize) -> Self { self.process_cap = cap; self }
    pub fn output_cap(mut self, cap: usize) -> Self { self.output_cap = cap; self }
    pub fn process_workers(mut self, workers: usize) -> Self { self.process_workers = workers; self }
    pub fn filter_workers(mut self, workers: usize) -> Self { self.filter_workers = workers; self }
    pub fn process_send_mode(mut self, mode: SendMode) -> Self { self.process_send_mode = mode; self }
    pub fn filter_send_mode(mut self, mode: SendMode) -> Self { self.filter_send_mode = mode; self }

    pub fn log_fn<F>(mut self, f: F) -> Self
    where
        F: Fn(&str) + Send + Sync + 'static,
    {
        self.log_fn = Some(Arc::new(f));
        self
    }

    pub fn build<T, E, F, G, Fut1, Fut2>(
        self,
        process_fn: F,
        filter_fn: G,
    ) -> (Pipeline<T>, ItemReceiver<Result<T, E>>, Vec<JoinHandle<()>>)
    where
        T: Send + 'static,
        E: Send + 'static,
        F: Fn(T) -> Fut1 + Send + Sync + 'static,
        Fut1: Future<Output = Result<T, E>> + Send + 'static,
        G: Fn(&T) -> Fut2 + Send + Sync + 'static,
        Fut2: Future<Output = Result<bool, E>> + Send + 'static,
    {
        let (tx_input, rx_input) = if self.input_cap == 0 {
            flume::unbounded()
        } else {
            flume::bounded(self.input_cap)
        };

        let (tx_process, rx_process) = if self.process_cap == 0 {
            flume::unbounded()
        } else {
            flume::bounded(self.process_cap)
        };

        let (tx_output, rx_output) = if self.output_cap == 0 {
            flume::unbounded()
        } else {
            flume::bounded(self.output_cap)
        };

        let process_fn = Arc::new(process_fn);
        let filter_fn = Arc::new(filter_fn);
        let log = self.log_fn;
        let process_send_mode = self.process_send_mode;
        let filter_send_mode = self.filter_send_mode;

        let mut tasks = Vec::with_capacity(self.process_workers + self.filter_workers);

        for _ in 0..self.process_workers {
            let rx_input   = rx_input.clone();
            let tx_process = tx_process.clone();
            let process_fn = process_fn.clone();
            let log        = log.clone();
            let mode       = process_send_mode;

            let task = task::spawn(async move {
                while let Ok(item) = rx_input.recv_async().await {
                    if let Some(l) = &log { l("process: starting"); }
                    let result = process_fn(item).await;
                    if let Some(l) = &log { l("process: complete"); }

                    let sent = match mode {
                        SendMode::Blocking => tx_process.send_async(result).await.is_ok(),
                        SendMode::TryDrop  => tx_process.try_send(result).is_ok(),
                    };
                    if !sent && matches!(mode, SendMode::TryDrop) {
                        if let Some(l) = &log { l("process: dropped (channel full)"); }
                    }
                }
                if let Some(l) = &log { l("process worker exited"); }
            });
            tasks.push(task);
        }

        for _ in 0..self.filter_workers {
            let rx_process = rx_process.clone();
            let tx_output  = tx_output.clone();
            let filter_fn  = filter_fn.clone();
            let log        = log.clone();
            let mode       = filter_send_mode;

            let task = task::spawn(async move {
                while let Ok(result) = rx_process.recv_async().await {
                    if let Some(l) = &log { l("filter: starting"); }

                    let to_send = match result {
                        Ok(t) => match filter_fn(&t).await {
                            Ok(true)  => { if let Some(l) = &log { l("filter: passed"); } Ok(t) }
                            Ok(false) => { if let Some(l) = &log { l("filter: dropped"); } continue; }
                            Err(e)    => Err(e),
                        },
                        Err(e) => Err(e),
                    };

                    if let Some(l) = &log { l("filter: complete"); }

                    let sent = match mode {
                        SendMode::Blocking => tx_output.send_async(to_send).await.is_ok(),
                        SendMode::TryDrop  => tx_output.try_send(to_send).is_ok(),
                    };
                    if !sent && matches!(mode, SendMode::TryDrop) {
                        if let Some(l) = &log { l("filter: dropped (channel full)"); }
                    }
                }
                if let Some(l) = &log { l("filter worker exited"); }
            });
            tasks.push(task);
        }

        (Pipeline { tx_input }, rx_output, tasks)
    }
}

pub struct Pipeline<T> {
    pub tx_input: ItemSender<T>,
}