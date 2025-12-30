//! Image transformation for storage objects

use serde::{Deserialize, Serialize};

/// Image transformation options
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct TransformOptions {
    /// Target width in pixels
    pub width: Option<u32>,
    /// Target height in pixels
    pub height: Option<u32>,
    /// Resize mode
    pub resize: Option<ResizeMode>,
    /// Output format
    pub format: Option<ImageFormat>,
    /// Quality (1-100)
    pub quality: Option<u8>,
}

/// Resize modes for image transformation
#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ResizeMode {
    /// Resize to cover the dimensions
    Cover,
    /// Resize to fit within dimensions
    Contain,
    /// Fill to exact dimensions (may distort)
    Fill,
}

/// Output image formats
#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ImageFormat {
    Jpeg,
    Png,
    Webp,
    Avif,
    Original,
}

impl TransformOptions {
    /// Parse from URL query parameters
    pub fn from_query(query: &str) -> Self {
        let mut opts = Self::default();

        for pair in query.split('&') {
            if let Some((key, value)) = pair.split_once('=') {
                match key {
                    "width" | "w" => opts.width = value.parse().ok(),
                    "height" | "h" => opts.height = value.parse().ok(),
                    "resize" => {
                        opts.resize = match value {
                            "cover" => Some(ResizeMode::Cover),
                            "contain" => Some(ResizeMode::Contain),
                            "fill" => Some(ResizeMode::Fill),
                            _ => None,
                        }
                    }
                    "format" | "f" => {
                        opts.format = match value {
                            "jpeg" | "jpg" => Some(ImageFormat::Jpeg),
                            "png" => Some(ImageFormat::Png),
                            "webp" => Some(ImageFormat::Webp),
                            "avif" => Some(ImageFormat::Avif),
                            "origin" | "original" => Some(ImageFormat::Original),
                            _ => None,
                        }
                    }
                    "quality" | "q" => opts.quality = value.parse().ok(),
                    _ => {}
                }
            }
        }

        opts
    }

    /// Check if any transformations are requested
    pub fn is_empty(&self) -> bool {
        self.width.is_none()
            && self.height.is_none()
            && self.resize.is_none()
            && self.format.is_none()
            && self.quality.is_none()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_transform_options() {
        let opts = TransformOptions::from_query("width=200&height=100&format=webp&quality=80");
        assert_eq!(opts.width, Some(200));
        assert_eq!(opts.height, Some(100));
        assert!(matches!(opts.format, Some(ImageFormat::Webp)));
        assert_eq!(opts.quality, Some(80));
    }
}
