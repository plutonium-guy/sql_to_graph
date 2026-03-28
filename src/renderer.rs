use crate::error::{Result, SqlToGraphError};

pub fn svg_to_raster(
    svg_data: &str,
    width: u32,
    height: u32,
    format: image::ImageFormat,
) -> Result<Vec<u8>> {
    let tree = resvg::usvg::Tree::from_str(svg_data, &resvg::usvg::Options::default())
        .map_err(|e| SqlToGraphError::ImageError(format!("SVG parse error: {}", e)))?;

    let mut pixmap = tiny_skia::Pixmap::new(width, height)
        .ok_or_else(|| SqlToGraphError::ImageError("Failed to create pixmap".into()))?;

    // Fill with white background
    pixmap.fill(tiny_skia::Color::WHITE);

    let scale_x = width as f32 / tree.size().width();
    let scale_y = height as f32 / tree.size().height();
    let scale = scale_x.min(scale_y);

    let transform = tiny_skia::Transform::from_scale(scale, scale);

    resvg::render(&tree, transform, &mut pixmap.as_mut());

    let img = image::RgbaImage::from_raw(width, height, pixmap.data().to_vec())
        .ok_or_else(|| SqlToGraphError::ImageError("Failed to create image buffer".into()))?;

    let mut buf = std::io::Cursor::new(Vec::new());
    img.write_to(&mut buf, format)
        .map_err(|e| SqlToGraphError::ImageError(format!("Image encoding error: {}", e)))?;

    Ok(buf.into_inner())
}
