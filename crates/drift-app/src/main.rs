use drift_app::{logging, AppSendController, AppState};

fn main() {
    logging::init();
    let startup_error = match AppState::bootstrap() {
        Ok(app_state) => {
            tracing::info!(
                config_source = app_state.settings_source().as_str(),
                backend = app_state.backend_name(),
                custom_relay = app_state.custom_relay_configured(),
                "drift services initialized"
            );
            drift_ui::run_with_controller(std::sync::Arc::new(AppSendController::new(
                app_state.handle(),
            )));
            return;
        }
        Err(error) => {
            tracing::error!(error = %error, "drift startup failed");
            Some(error.user_message().to_owned())
        }
    };

    drift_ui::run_with_startup_error(startup_error);
}
