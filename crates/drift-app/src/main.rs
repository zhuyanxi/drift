mod logging;

fn main() {
    logging::init();
    tracing::info!("starting drift");
    drift_ui::run();
}
