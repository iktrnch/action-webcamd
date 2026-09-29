mod hotplug;
mod settings;
mod virtual_camera;

use anyhow::Result;
use tracing_subscriber::EnvFilter;

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<()> {
    init_tracing();
    let (virtual_camera, producer_failures) = virtual_camera::VirtualCamera::prepare()?;
    hotplug::run(virtual_camera, producer_failures).await
}

fn init_tracing() {
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));

    tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_target(false)
        .init();
}
