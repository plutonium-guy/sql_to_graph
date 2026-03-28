use plotters::prelude::*;
use pyo3::prelude::*;

use crate::error::{Result, SqlToGraphError};
use crate::renderer::svg_to_raster;
use crate::types::{
    CellValueInner, ChartConfig, ChartOutput, ChartType, OutputFormat, QueryResult,
};

const PALETTE: [RGBColor; 10] = [
    RGBColor(66, 133, 244),
    RGBColor(234, 67, 53),
    RGBColor(251, 188, 4),
    RGBColor(52, 168, 83),
    RGBColor(255, 109, 0),
    RGBColor(156, 39, 176),
    RGBColor(0, 188, 212),
    RGBColor(139, 195, 74),
    RGBColor(255, 87, 34),
    RGBColor(63, 81, 181),
];

pub fn render_chart_internal(result: &QueryResult, config: &ChartConfig) -> Result<ChartOutput> {
    let x_idx = col_index(result, &config.x_column)?;
    let y_idx = col_index(result, &config.y_column)?;
    let z_idx = config
        .z_column
        .as_ref()
        .map(|z| col_index(result, z))
        .transpose()?;

    let labels: Vec<String> = result.rows.iter().map(|r| cell_to_string(&r[x_idx])).collect();
    let values: Vec<f64> = result.rows.iter().map(|r| cell_to_f64(&r[y_idx])).collect();
    let z_values: Option<Vec<f64>> =
        z_idx.map(|zi| result.rows.iter().map(|r| cell_to_f64(&r[zi])).collect());

    if labels.is_empty() || values.is_empty() {
        return Err(SqlToGraphError::ChartError("No data to chart".into()));
    }

    let svg_data = render_svg(&labels, &values, z_values.as_deref(), config)?;
    to_output(svg_data, config)
}

fn col_index(result: &QueryResult, name: &str) -> Result<usize> {
    result
        .columns
        .iter()
        .position(|c| c == name)
        .ok_or_else(|| SqlToGraphError::ChartError(format!("Column '{}' not found", name)))
}

fn to_output(svg_data: String, config: &ChartConfig) -> Result<ChartOutput> {
    match config.output_format {
        OutputFormat::Svg => Ok(ChartOutput {
            format: OutputFormat::Svg,
            data: svg_data.into_bytes(),
            mime_type: "image/svg+xml".into(),
        }),
        OutputFormat::Html => {
            let html = format!(
                r#"<!DOCTYPE html>
<html><head><meta charset="utf-8"><title>{}</title>
<style>body{{margin:0;display:flex;justify-content:center;align-items:center;min-height:100vh;background:#f5f5f5}}svg{{max-width:100%;height:auto}}</style>
</head><body>{}</body></html>"#,
                config.title.as_deref().unwrap_or("Chart"),
                svg_data
            );
            Ok(ChartOutput {
                format: OutputFormat::Html,
                data: html.into_bytes(),
                mime_type: "text/html".into(),
            })
        }
        OutputFormat::Png => Ok(ChartOutput {
            format: OutputFormat::Png,
            data: svg_to_raster(&svg_data, config.width, config.height, image::ImageFormat::Png)?,
            mime_type: "image/png".into(),
        }),
        OutputFormat::Jpg => Ok(ChartOutput {
            format: OutputFormat::Jpg,
            data: svg_to_raster(
                &svg_data,
                config.width,
                config.height,
                image::ImageFormat::Jpeg,
            )?,
            mime_type: "image/jpeg".into(),
        }),
    }
}

fn y_range(values: &[f64]) -> (f64, f64) {
    let y_min = values.iter().cloned().fold(f64::INFINITY, f64::min);
    let y_max = values.iter().cloned().fold(f64::NEG_INFINITY, f64::max);
    let pad = (y_max - y_min).abs() * 0.1;
    let lo = if y_min >= 0.0 { 0.0 } else { y_min - pad };
    (lo, y_max + pad)
}

fn render_svg(
    labels: &[String],
    values: &[f64],
    z_values: Option<&[f64]>,
    config: &ChartConfig,
) -> Result<String> {
    let mut svg_buf = String::new();
    {
        let root = SVGBackend::with_string(&mut svg_buf, (config.width, config.height))
            .into_drawing_area();
        root.fill(&WHITE).map_err(chart_err)?;

        let title = config.title.as_deref().unwrap_or("Query Result");

        match config.chart_type {
            ChartType::Bar => render_bar(&root, title, labels, values)?,
            ChartType::HorizontalBar => render_horizontal_bar(&root, title, labels, values)?,
            ChartType::StackedBar => {
                render_stacked_bar(&root, title, labels, values, z_values)?
            }
            ChartType::Line => render_line(&root, title, labels, values)?,
            ChartType::Area => render_area(&root, title, labels, values)?,
            ChartType::Pie => render_pie(&root, title, labels, values, false)?,
            ChartType::Donut => render_pie(&root, title, labels, values, true)?,
            ChartType::Scatter => render_scatter(&root, title, labels, values)?,
            ChartType::Histogram => render_histogram(&root, title, values, config.bin_count)?,
            ChartType::Heatmap => {
                render_heatmap(&root, title, labels, values, z_values)?
            }
        }

        root.present().map_err(chart_err)?;
    }
    Ok(svg_buf)
}

fn chart_err<E: std::fmt::Display>(e: E) -> SqlToGraphError {
    SqlToGraphError::ChartError(e.to_string())
}

// ─── Bar (vertical) ─────────────────────────────────────────────────────────

fn render_bar(
    root: &DrawingArea<SVGBackend, plotters::coord::Shift>,
    title: &str,
    labels: &[String],
    values: &[f64],
) -> Result<()> {
    let (y_lo, y_hi) = y_range(values);
    let mut chart = ChartBuilder::on(root)
        .caption(title, ("sans-serif", 24))
        .margin(10)
        .x_label_area_size(60)
        .y_label_area_size(60)
        .build_cartesian_2d(0..labels.len(), y_lo..y_hi)
        .map_err(chart_err)?;

    chart
        .configure_mesh()
        .x_labels(labels.len().min(20))
        .x_label_formatter(&|i| labels.get(*i).cloned().unwrap_or_default())
        .y_desc("Value")
        .draw()
        .map_err(chart_err)?;

    chart
        .draw_series(values.iter().enumerate().map(|(i, &v)| {
            Rectangle::new([(i, 0.0), (i + 1, v)], PALETTE[i % PALETTE.len()].mix(0.85).filled())
        }))
        .map_err(chart_err)?;

    Ok(())
}

// ─── Horizontal Bar ─────────────────────────────────────────────────────────

fn render_horizontal_bar(
    root: &DrawingArea<SVGBackend, plotters::coord::Shift>,
    title: &str,
    labels: &[String],
    values: &[f64],
) -> Result<()> {
    let x_max = values.iter().cloned().fold(f64::NEG_INFINITY, f64::max) * 1.1;
    let x_max = if x_max <= 0.0 { 1.0 } else { x_max };

    let mut chart = ChartBuilder::on(root)
        .caption(title, ("sans-serif", 24))
        .margin(10)
        .x_label_area_size(40)
        .y_label_area_size(120)
        .build_cartesian_2d(0.0..x_max, 0..labels.len())
        .map_err(chart_err)?;

    chart
        .configure_mesh()
        .y_labels(labels.len().min(20))
        .y_label_formatter(&|i| labels.get(*i).cloned().unwrap_or_default())
        .x_desc("Value")
        .draw()
        .map_err(chart_err)?;

    chart
        .draw_series(values.iter().enumerate().map(|(i, &v)| {
            Rectangle::new(
                [(0.0, i), (v, i + 1)],
                PALETTE[i % PALETTE.len()].mix(0.85).filled(),
            )
        }))
        .map_err(chart_err)?;

    Ok(())
}

// ─── Stacked Bar ────────────────────────────────────────────────────────────

fn render_stacked_bar(
    root: &DrawingArea<SVGBackend, plotters::coord::Shift>,
    title: &str,
    labels: &[String],
    values: &[f64],
    z_values: Option<&[f64]>,
) -> Result<()> {
    let z = z_values.ok_or_else(|| {
        SqlToGraphError::ChartError(
            "StackedBar requires z_column for the second series".into(),
        )
    })?;

    let stacked: Vec<f64> = values.iter().zip(z.iter()).map(|(a, b)| a + b).collect();
    let y_max = stacked.iter().cloned().fold(f64::NEG_INFINITY, f64::max) * 1.1;
    let y_max = if y_max <= 0.0 { 1.0 } else { y_max };

    let mut chart = ChartBuilder::on(root)
        .caption(title, ("sans-serif", 24))
        .margin(10)
        .x_label_area_size(60)
        .y_label_area_size(60)
        .build_cartesian_2d(0..labels.len(), 0.0..y_max)
        .map_err(chart_err)?;

    chart
        .configure_mesh()
        .x_labels(labels.len().min(20))
        .x_label_formatter(&|i| labels.get(*i).cloned().unwrap_or_default())
        .y_desc("Value")
        .draw()
        .map_err(chart_err)?;

    // Bottom series (y_column)
    chart
        .draw_series(values.iter().enumerate().map(|(i, &v)| {
            Rectangle::new([(i, 0.0), (i + 1, v)], PALETTE[0].mix(0.85).filled())
        }))
        .map_err(chart_err)?
        .label("Series 1")
        .legend(|(x, y)| Rectangle::new([(x, y - 5), (x + 15, y + 5)], PALETTE[0].filled()));

    // Top series (z_column)
    chart
        .draw_series(
            values
                .iter()
                .zip(z.iter())
                .enumerate()
                .map(|(i, (&base, &top))| {
                    Rectangle::new(
                        [(i, base), (i + 1, base + top)],
                        PALETTE[1].mix(0.85).filled(),
                    )
                }),
        )
        .map_err(chart_err)?
        .label("Series 2")
        .legend(|(x, y)| Rectangle::new([(x, y - 5), (x + 15, y + 5)], PALETTE[1].filled()));

    chart
        .configure_series_labels()
        .background_style(WHITE.mix(0.8))
        .border_style(BLACK)
        .draw()
        .map_err(chart_err)?;

    Ok(())
}

// ─── Line ───────────────────────────────────────────────────────────────────

fn render_line(
    root: &DrawingArea<SVGBackend, plotters::coord::Shift>,
    title: &str,
    labels: &[String],
    values: &[f64],
) -> Result<()> {
    let (y_lo, y_hi) = y_range(values);
    let mut chart = ChartBuilder::on(root)
        .caption(title, ("sans-serif", 24))
        .margin(10)
        .x_label_area_size(60)
        .y_label_area_size(60)
        .build_cartesian_2d(0f64..(labels.len() as f64), y_lo..y_hi)
        .map_err(chart_err)?;

    chart
        .configure_mesh()
        .x_labels(labels.len().min(20))
        .x_label_formatter(&|x| labels.get(*x as usize).cloned().unwrap_or_default())
        .draw()
        .map_err(chart_err)?;

    let data: Vec<(f64, f64)> = values.iter().enumerate().map(|(i, &v)| (i as f64, v)).collect();

    chart
        .draw_series(LineSeries::new(data.clone(), PALETTE[0].stroke_width(2)))
        .map_err(chart_err)?;

    chart
        .draw_series(
            data.iter()
                .map(|&(x, y)| Circle::new((x, y), 4, PALETTE[0].filled())),
        )
        .map_err(chart_err)?;

    Ok(())
}

// ─── Area ───────────────────────────────────────────────────────────────────

fn render_area(
    root: &DrawingArea<SVGBackend, plotters::coord::Shift>,
    title: &str,
    labels: &[String],
    values: &[f64],
) -> Result<()> {
    let (y_lo, y_hi) = y_range(values);
    let mut chart = ChartBuilder::on(root)
        .caption(title, ("sans-serif", 24))
        .margin(10)
        .x_label_area_size(60)
        .y_label_area_size(60)
        .build_cartesian_2d(0f64..(labels.len() as f64), y_lo..y_hi)
        .map_err(chart_err)?;

    chart
        .configure_mesh()
        .x_labels(labels.len().min(20))
        .x_label_formatter(&|x| labels.get(*x as usize).cloned().unwrap_or_default())
        .draw()
        .map_err(chart_err)?;

    let data: Vec<(f64, f64)> = values.iter().enumerate().map(|(i, &v)| (i as f64, v)).collect();

    chart
        .draw_series(AreaSeries::new(
            data.clone(),
            0.0,
            PALETTE[0].mix(0.3),
        ).border_style(PALETTE[0].stroke_width(2)))
        .map_err(chart_err)?;

    chart
        .draw_series(
            data.iter()
                .map(|&(x, y)| Circle::new((x, y), 3, PALETTE[0].filled())),
        )
        .map_err(chart_err)?;

    Ok(())
}

// ─── Pie / Donut ────────────────────────────────────────────────────────────

fn render_pie(
    root: &DrawingArea<SVGBackend, plotters::coord::Shift>,
    title: &str,
    labels: &[String],
    values: &[f64],
    donut: bool,
) -> Result<()> {
    let (w, h) = root.dim_in_pixel();
    let cx = w as f64 / 2.0;
    let cy = h as f64 / 2.0 + 20.0;
    let radius = (w.min(h) as f64 / 2.0) * 0.7;
    let inner_radius = if donut { radius * 0.45 } else { 0.0 };

    root.draw(&Text::new(
        title.to_string(),
        (w as i32 / 2 - (title.len() as i32 * 6), 10),
        ("sans-serif", 24).into_font(),
    ))
    .map_err(chart_err)?;

    let total: f64 = values.iter().sum();
    if total == 0.0 {
        return Err(SqlToGraphError::ChartError(
            "All values are zero, cannot draw pie chart".into(),
        ));
    }

    let mut start_angle: f64 = -std::f64::consts::FRAC_PI_2; // start at 12 o'clock
    for (i, (&val, label)) in values.iter().zip(labels.iter()).enumerate() {
        let sweep = (val / total) * 2.0 * std::f64::consts::PI;
        let color = PALETTE[i % PALETTE.len()];

        // Build polygon for the slice (outer arc, then inner arc reversed for donut)
        let steps = (sweep * 50.0).max(2.0) as usize;
        let mut points: Vec<(i32, i32)> = Vec::new();

        if donut {
            // Outer arc forward
            for step in 0..=steps {
                let angle = start_angle + (sweep * step as f64 / steps as f64);
                points.push(((cx + radius * angle.cos()) as i32, (cy + radius * angle.sin()) as i32));
            }
            // Inner arc backward
            for step in (0..=steps).rev() {
                let angle = start_angle + (sweep * step as f64 / steps as f64);
                points.push(((cx + inner_radius * angle.cos()) as i32, (cy + inner_radius * angle.sin()) as i32));
            }
        } else {
            points.push((cx as i32, cy as i32));
            for step in 0..=steps {
                let angle = start_angle + (sweep * step as f64 / steps as f64);
                points.push(((cx + radius * angle.cos()) as i32, (cy + radius * angle.sin()) as i32));
            }
        }

        root.draw(&Polygon::new(points, color.filled()))
            .map_err(chart_err)?;

        // Label at midpoint of arc
        let mid_angle = start_angle + sweep / 2.0;
        let label_r = if donut {
            (radius + inner_radius) / 2.0
        } else {
            radius * 0.65
        };
        let lx = cx + label_r * mid_angle.cos();
        let ly = cy + label_r * mid_angle.sin();

        let pct = (val / total * 100.0) as u32;
        if pct >= 3 {
            // Only label slices >= 3%
            let display = format!("{} ({}%)", label, pct);
            root.draw(&Text::new(
                display,
                (lx as i32, ly as i32),
                ("sans-serif", 11).into_font().color(&WHITE),
            ))
            .map_err(chart_err)?;
        }

        start_angle += sweep;
    }

    // Legend on the right
    let legend_x = w as i32 - 150;
    let mut legend_y = 50;
    for (i, (label, &val)) in labels.iter().zip(values.iter()).enumerate() {
        let color = PALETTE[i % PALETTE.len()];
        let pct = (val / total * 100.0) as u32;
        root.draw(&Rectangle::new(
            [(legend_x, legend_y), (legend_x + 12, legend_y + 12)],
            color.filled(),
        ))
        .map_err(chart_err)?;
        root.draw(&Text::new(
            format!("{} ({}%)", label, pct),
            (legend_x + 16, legend_y),
            ("sans-serif", 11).into_font(),
        ))
        .map_err(chart_err)?;
        legend_y += 18;
    }

    Ok(())
}

// ─── Scatter ────────────────────────────────────────────────────────────────

fn render_scatter(
    root: &DrawingArea<SVGBackend, plotters::coord::Shift>,
    title: &str,
    labels: &[String],
    values: &[f64],
) -> Result<()> {
    // x-axis: try parse labels as f64 for true scatter, fall back to index
    let x_vals: Vec<f64> = labels
        .iter()
        .enumerate()
        .map(|(i, l)| l.parse::<f64>().unwrap_or(i as f64))
        .collect();

    let x_min = x_vals.iter().cloned().fold(f64::INFINITY, f64::min);
    let x_max = x_vals.iter().cloned().fold(f64::NEG_INFINITY, f64::max);
    let x_pad = (x_max - x_min).abs() * 0.05;
    let (y_lo, y_hi) = y_range(values);

    let mut chart = ChartBuilder::on(root)
        .caption(title, ("sans-serif", 24))
        .margin(10)
        .x_label_area_size(40)
        .y_label_area_size(60)
        .build_cartesian_2d((x_min - x_pad)..(x_max + x_pad), y_lo..y_hi)
        .map_err(chart_err)?;

    chart.configure_mesh().draw().map_err(chart_err)?;

    chart
        .draw_series(
            x_vals
                .iter()
                .zip(values.iter())
                .map(|(&x, &y)| Circle::new((x, y), 5, PALETTE[0].mix(0.7).filled())),
        )
        .map_err(chart_err)?;

    Ok(())
}

// ─── Histogram (auto-binning) ───────────────────────────────────────────────

fn render_histogram(
    root: &DrawingArea<SVGBackend, plotters::coord::Shift>,
    title: &str,
    values: &[f64],
    bin_count: u32,
) -> Result<()> {
    let v_min = values.iter().cloned().fold(f64::INFINITY, f64::min);
    let v_max = values.iter().cloned().fold(f64::NEG_INFINITY, f64::max);

    if (v_max - v_min).abs() < f64::EPSILON {
        return Err(SqlToGraphError::ChartError(
            "All values are identical, cannot create histogram bins".into(),
        ));
    }

    let bins = bin_count.max(1) as usize;
    let bin_width = (v_max - v_min) / bins as f64;

    // Count values per bin
    let mut counts = vec![0u32; bins];
    for &v in values {
        let idx = ((v - v_min) / bin_width) as usize;
        let idx = idx.min(bins - 1); // clamp max value into last bin
        counts[idx] += 1;
    }

    let max_count = *counts.iter().max().unwrap_or(&1);

    let mut chart = ChartBuilder::on(root)
        .caption(title, ("sans-serif", 24))
        .margin(10)
        .x_label_area_size(40)
        .y_label_area_size(50)
        .build_cartesian_2d(v_min..v_max, 0u32..(max_count + 1))
        .map_err(chart_err)?;

    chart
        .configure_mesh()
        .x_desc("Value")
        .y_desc("Count")
        .draw()
        .map_err(chart_err)?;

    chart
        .draw_series(counts.iter().enumerate().map(|(i, &count)| {
            let lo = v_min + i as f64 * bin_width;
            let hi = lo + bin_width;
            Rectangle::new([(lo, 0), (hi, count)], PALETTE[0].mix(0.8).filled())
        }))
        .map_err(chart_err)?;

    // Draw bin edges
    chart
        .draw_series(counts.iter().enumerate().map(|(i, &count)| {
            let lo = v_min + i as f64 * bin_width;
            let hi = lo + bin_width;
            Rectangle::new([(lo, 0), (hi, count)], PALETTE[0].stroke_width(1))
        }))
        .map_err(chart_err)?;

    Ok(())
}

// ─── Heatmap ────────────────────────────────────────────────────────────────

fn render_heatmap(
    root: &DrawingArea<SVGBackend, plotters::coord::Shift>,
    title: &str,
    labels: &[String],
    values: &[f64],
    z_values: Option<&[f64]>,
) -> Result<()> {
    // Heatmap: x_column = x labels, y_column = y labels, z_column = intensity
    // If no z_column, use values as intensity with x as labels and y as row index
    let z = z_values.unwrap_or(values);

    // Deduplicate to get unique x and y labels
    let x_labels: Vec<String> = {
        let mut seen = Vec::new();
        for l in labels {
            if !seen.contains(l) {
                seen.push(l.clone());
            }
        }
        seen
    };

    let y_labels: Vec<String> = {
        let mut seen = Vec::new();
        for v in values {
            let s = format!("{:.2}", v);
            if !seen.contains(&s) {
                seen.push(s);
            }
        }
        seen
    };

    // Build grid: map (x_label, y_value) -> z intensity
    let z_min = z.iter().cloned().fold(f64::INFINITY, f64::min);
    let z_max = z.iter().cloned().fold(f64::NEG_INFINITY, f64::max);
    let z_range = if (z_max - z_min).abs() < f64::EPSILON {
        1.0
    } else {
        z_max - z_min
    };

    let x_count = x_labels.len();
    let y_count = y_labels.len();

    let mut chart = ChartBuilder::on(root)
        .caption(title, ("sans-serif", 24))
        .margin(10)
        .x_label_area_size(60)
        .y_label_area_size(80)
        .build_cartesian_2d(0..x_count, 0..y_count)
        .map_err(chart_err)?;

    chart
        .configure_mesh()
        .x_labels(x_count.min(20))
        .x_label_formatter(&|i| x_labels.get(*i).cloned().unwrap_or_default())
        .y_labels(y_count.min(20))
        .y_label_formatter(&|i| y_labels.get(*i).cloned().unwrap_or_default())
        .disable_mesh()
        .draw()
        .map_err(chart_err)?;

    // Draw cells
    for (idx, &intensity) in z.iter().enumerate() {
        let xi = x_labels
            .iter()
            .position(|l| l == &labels[idx % labels.len()])
            .unwrap_or(0);
        let yi = y_labels
            .iter()
            .position(|l| l == &format!("{:.2}", values[idx % values.len()]))
            .unwrap_or(0);

        let normalized = (intensity - z_min) / z_range; // 0..1
        // Blue -> Red gradient
        let r = (normalized * 255.0) as u8;
        let b = ((1.0 - normalized) * 255.0) as u8;
        let g = ((0.5 - (normalized - 0.5).abs()) * 255.0 * 2.0).max(0.0) as u8;
        let color = RGBColor(r, g, b);

        chart
            .draw_series(std::iter::once(Rectangle::new(
                [(xi, yi), (xi + 1, yi + 1)],
                color.filled(),
            )))
            .map_err(chart_err)?;
    }

    Ok(())
}

// ─── Helpers ────────────────────────────────────────────────────────────────

fn cell_to_string(cell: &crate::types::CellValue) -> String {
    match &cell.value {
        CellValueInner::Null => "NULL".to_string(),
        CellValueInner::Bool(b) => b.to_string(),
        CellValueInner::Int(i) => i.to_string(),
        CellValueInner::Float(f) => format!("{:.2}", f),
        CellValueInner::Text(s) => s.clone(),
    }
}

fn cell_to_f64(cell: &crate::types::CellValue) -> f64 {
    match &cell.value {
        CellValueInner::Int(i) => *i as f64,
        CellValueInner::Float(f) => *f,
        CellValueInner::Text(s) => s.parse().unwrap_or(0.0),
        CellValueInner::Bool(b) => if *b { 1.0 } else { 0.0 },
        CellValueInner::Null => 0.0,
    }
}

#[pyfunction]
pub fn render_chart(result: &QueryResult, config: &ChartConfig) -> PyResult<ChartOutput> {
    Ok(render_chart_internal(result, config)?)
}
