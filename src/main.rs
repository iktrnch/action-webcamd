mod gopro;
mod hotplug;
mod network;
mod settings;
mod stream;
mod virtual_camera;

use anyhow::Result;
use tracing_subscriber::EnvFilter;

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<()> {
    init_tracing();
    let settings = settings::Settings::load()?;
    let (virtual_camera, producer_failures) =
        virtual_camera::VirtualCamera::prepare(settings.virtual_camera)?;
    hotplug::run(virtual_camera, producer_failures).await
}

fn init_tracing() {
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));

    tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_target(false)
        .init();
}
