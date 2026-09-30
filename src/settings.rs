use std::{
    fs::{self, OpenOptions},
    io::{self, Write},
    path::Path,
};

use anyhow::{Context, Result, ensure};
use serde::{Deserialize, Serialize};

/// Absolute path to the daemon configuration file.
pub(crate) const CONFIG_PATH: &str = "/etc/action-webcamd/config.toml";

/// Initial virtual-camera width in pixels.
pub(crate) const VIDEO_WIDTH: u32 = 1920;

/// Initial virtual-camera height in pixels.
pub(crate) const VIDEO_HEIGHT: u32 = 1080;

/// Initial virtual-camera frame rate in frames per second.
pub(crate) const VIDEO_FPS: u32 = 30;

/// Configuration loaded once when the daemon starts.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Settings {
    pub(crate) virtual_camera: VirtualCameraSettings,
}

/// Settings for the administrator-provisioned v4l2loopback device.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct VirtualCameraSettings {
    /// Card label used to identify the daemon's persistent v4l2loopback device.
    pub(crate) label: String,
}

impl Default for Settings {
    fn default() -> Self {
        Self {
            virtual_camera: VirtualCameraSettings {
                label: "Action Webcam".to_owned(),
            },
        }
    }
}

impl Settings {
    /// Loads the system configuration, creating its default file on first run.
    pub(crate) fn load() -> Result<Self> {
        Self::load_from(Path::new(CONFIG_PATH))
    }

    fn load_from(path: &Path) -> Result<Self> {
        match Self::read_from(path) {
            Ok(settings) => Ok(settings),
            Err(error) if error.kind() == io::ErrorKind::NotFound => {
                if create_default_config(path)? {
                    Ok(Self::default())
                } else {
                    Self::read_from(path).with_context(|| {
                        format!(
                            "failed to read configuration created concurrently at {}",
                            path.display()
                        )
                    })
                }
            }
            Err(error) => Err(error)
                .with_context(|| format!("failed to load configuration from {}", path.display())),
        }
    }

    fn read_from(path: &Path) -> std::result::Result<Self, io::Error> {
        let contents = fs::read_to_string(path)?;
        let settings = toml::from_str::<Self>(&contents).map_err(|error| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                format!("failed to parse {}: {error}", path.display()),
            )
        })?;
        settings.validate().map_err(io::Error::other)?;
        Ok(settings)
    }

    fn validate(&self) -> Result<()> {
        ensure!(
            !self.virtual_camera.label.trim().is_empty(),
            "virtual_camera.label must not be blank"
        );
        Ok(())
    }
}

/// Creates the default configuration without overwriting one created elsewhere.
fn create_default_config(path: &Path) -> Result<bool> {
    let parent = path
        .parent()
        .context("configuration path has no parent directory")?;
    fs::create_dir_all(parent).with_context(|| {
        format!(
            "failed to create configuration directory {}",
            parent.display()
        )
    })?;

    let mut file = match OpenOptions::new().write(true).create_new(true).open(path) {
        Ok(file) => file,
        Err(error) if error.kind() == io::ErrorKind::AlreadyExists => return Ok(false),
        Err(error) => {
            return Err(error).with_context(|| {
                format!(
                    "failed to create default configuration at {}",
                    path.display()
                )
            });
        }
    };
    let contents = toml::to_string_pretty(&Settings::default())
        .context("failed to serialize default configuration")?;
    file.write_all(contents.as_bytes()).with_context(|| {
        format!(
            "failed to write default configuration at {}",
            path.display()
        )
    })?;
    file.sync_all().with_context(|| {
        format!(
            "failed to persist default configuration at {}",
            path.display()
        )
    })?;
    Ok(true)
}

#[cfg(test)]
mod tests {
    use std::{
        fs,
        sync::atomic::{AtomicUsize, Ordering},
    };

    use super::*;

    static NEXT_TEMP_DIRECTORY: AtomicUsize = AtomicUsize::new(0);

    struct TempDirectory {
        path: std::path::PathBuf,
    }

    impl TempDirectory {
        fn new() -> Self {
            let sequence = NEXT_TEMP_DIRECTORY.fetch_add(1, Ordering::Relaxed);
            let path = std::env::temp_dir().join(format!(
                "action-webcamd-settings-{}-{sequence}",
                std::process::id()
            ));
            fs::create_dir_all(&path).unwrap();
            Self { path }
        }
    }

    impl Drop for TempDirectory {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.path);
        }
    }

    fn config_path(root: &TempDirectory) -> std::path::PathBuf {
        root.path.join("etc/action-webcamd/config.toml")
    }

    #[test]
    fn missing_configuration_creates_the_default_file_and_parent_directories() {
        let root = TempDirectory::new();
        let path = config_path(&root);

        assert_eq!(Settings::load_from(&path).unwrap(), Settings::default());
        assert_eq!(
            fs::read_to_string(path).unwrap(),
            "[virtual_camera]\nlabel = \"Action Webcam\"\n"
        );
    }

    #[test]
    fn parses_a_valid_configuration() {
        let root = TempDirectory::new();
        let path = config_path(&root);
        fs::create_dir_all(path.parent().unwrap()).unwrap();
        fs::write(
            &path,
            "[virtual_camera]\nlabel = \"Office Action Camera\"\n",
        )
        .unwrap();

        assert_eq!(
            Settings::load_from(&path).unwrap(),
            Settings {
                virtual_camera: VirtualCameraSettings {
                    label: "Office Action Camera".to_owned(),
                },
            }
        );
    }

    #[test]
    fn rejects_malformed_configuration_without_rewriting_it() {
        let root = TempDirectory::new();
        let path = config_path(&root);
        let contents = "[virtual_camera\nlabel = \"Action Webcam\"\n";
        fs::create_dir_all(path.parent().unwrap()).unwrap();
        fs::write(&path, contents).unwrap();

        assert!(Settings::load_from(&path).is_err());
        assert_eq!(fs::read_to_string(path).unwrap(), contents);
    }

    #[test]
    fn rejects_missing_or_unknown_configuration_keys() {
        for contents in [
            "[virtual_camera]\n",
            "[virtual_camera]\nlabel = \"Action Webcam\"\nlabels = \"Other\"\n",
        ] {
            let root = TempDirectory::new();
            let path = config_path(&root);
            fs::create_dir_all(path.parent().unwrap()).unwrap();
            fs::write(&path, contents).unwrap();

            assert!(Settings::load_from(&path).is_err());
            assert_eq!(fs::read_to_string(path).unwrap(), contents);
        }
    }

    #[test]
    fn rejects_a_blank_camera_label() {
        let root = TempDirectory::new();
        let path = config_path(&root);
        fs::create_dir_all(path.parent().unwrap()).unwrap();
        fs::write(&path, "[virtual_camera]\nlabel = \"   \"\n").unwrap();

        let error = Settings::load_from(&path).unwrap_err();
        assert!(format!("{error:#}").contains("must not be blank"));
    }
}
