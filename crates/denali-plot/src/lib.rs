use leptos::{
    component, create_effect, create_node_ref, create_rw_signal,
    view, IntoView, RwSignal, SignalGetUntracked, SignalWith,
};
use leptos::html::Canvas;
use wasm_bindgen::JsCast;
use plotters_canvas::CanvasBackend;
use plotters::drawing::IntoDrawingArea;
use crate::candlestick::{Candle, draw_candlestick_chart};

mod candlestick;
mod data;

#[component]
fn Chart(title: String, data: RwSignal<Vec<Candle>>) -> impl IntoView {
    let canvas_ref = create_node_ref::<Canvas>();
    let title_view = title.clone();
    let title_chart = title;

    create_effect(move |_| {
        data.with(|_| ());

        let Some(canvas_elem) = canvas_ref.get() else { return };
        let canvas = canvas_elem.unchecked_ref::<web_sys::HtmlCanvasElement>();

        let title = title_chart.clone();

        let backend = CanvasBackend::with_canvas_object(canvas.clone()).unwrap();
        let root = backend.into_drawing_area();
        let _ = draw_candlestick_chart(&root, &data.get_untracked(), &title);
    });

    view! {
        <div style="display: flex; flex-direction: column; align-items: center; margin: 10px;">
            <h3>{title_view}</h3>
            <canvas _ref=canvas_ref width="500" height="300" style="border: 1px solid #333;"></canvas>
        </div>
    }
}

#[component]
pub fn App() -> impl IntoView {
    let btc_data  = create_rw_signal(Vec::<Candle>::new());
    let eth_data  = create_rw_signal(Vec::<Candle>::new());
    let sol_data  = create_rw_signal(Vec::<Candle>::new());
    let xrp_data  = create_rw_signal(Vec::<Candle>::new());
    let ada_data  = create_rw_signal(Vec::<Candle>::new());
    let doge_data = create_rw_signal(Vec::<Candle>::new());

    view! {
        <div style="display: flex; flex-direction: column; align-items: center;">
            <h1>"Dashboard"</h1>
            <div style="display: flex; flex-wrap: wrap; justify-content: space-around; width: 100%;">
                <Chart title="BTC/USD".to_string() data=btc_data />
                <Chart title="ETH/USD".to_string() data=eth_data />
                <Chart title="SOL/USD".to_string() data=sol_data />
                <Chart title="XRP/USD".to_string() data=xrp_data />
                <Chart title="ADA/USD".to_string() data=ada_data />
                <Chart title="DOGE/USD".to_string() data=doge_data />
            </div>
        </div>
    }
}

