use plotters::prelude::*;
use plotters::element::CandleStick;
use wasm_bindgen::JsValue;

#[derive(Clone, Copy)]
pub struct Candle {
    pub time:  f64,
    pub open:  f64,
    pub high:  f64,
    pub low:   f64,
    pub close: f64,
}

pub fn draw_candlestick_chart(
    root: &DrawingArea<plotters_canvas::CanvasBackend, plotters::coord::Shift>,
    data: &[Candle],
    title: &str,
) -> Result<(), JsValue> {
    if data.is_empty() {
        return Ok(());
    }

    root.fill(&WHITE).map_err(|e| JsValue::from_str(&e.to_string()))?;

    let min_time  = data.iter().map(|c| c.time ).fold(f64::INFINITY, f64::min);
    let max_time  = data.iter().map(|c| c.time ).fold(f64::NEG_INFINITY, f64::max);
    let min_price = data.iter().fold(f64::INFINITY,  |a, c| a.min(c.low));
    let max_price = data.iter().fold(f64::NEG_INFINITY, |a, c| a.max(c.high));

    let x_range = if min_time >= max_time { min_time-0.5..max_time+0.5 } else { min_time..max_time };
    let y_range = if min_price >= max_price { min_price-1.0..max_price+1.0 } else { min_price..max_price };

    let mut chart = ChartBuilder::on(root)
        .caption(title, ("sans-serif", 30))
        .set_label_area_size(LabelAreaPosition::Left,   60)
        .set_label_area_size(LabelAreaPosition::Bottom, 40)
        .build_cartesian_2d(x_range, y_range)
        .map_err(|e| JsValue::from_str(&e.to_string()))?;

    chart.configure_mesh()
        .draw()
        .map_err(|e| JsValue::from_str(&e.to_string()))?;

    let up   = GREEN.filled();
    let down = RED.filled();

    chart.draw_series(
        data.iter().map(|c| {
            CandleStick::new(c.time, c.open, c.high, c.low, c.close, up, down, 10)
        })
    ).map_err(|e| JsValue::from_str(&e.to_string()))?;

    root.present().map_err(|e| JsValue::from_str(&e.to_string()))?;

    Ok(())
}