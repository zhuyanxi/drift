#[cfg(feature = "gui")]
mod gui {
    use gpui::{
        div, prelude::*, App, Application, Context, IntoElement, Render, Window, WindowOptions,
    };

    #[derive(Default)]
    pub struct MainView {
        startup_error: Option<String>,
    }

    impl Render for MainView {
        fn render(&mut self, _window: &mut Window, _cx: &mut Context<Self>) -> impl IntoElement {
            let content = self
                .startup_error
                .as_deref()
                .map_or("drift".to_owned(), ToOwned::to_owned);
            div().size_full().child(content)
        }
    }

    pub fn run_with_startup_error(startup_error: Option<String>) {
        Application::new().run(move |cx: &mut App| {
            cx.open_window(WindowOptions::default(), |_, cx| {
                cx.new(|_| MainView { startup_error })
            })
            .expect("failed to open drift window");
            cx.activate(true);
        });
    }

    pub fn run() {
        run_with_startup_error(None);
    }
}

#[cfg(feature = "gui")]
pub use gui::{run, run_with_startup_error, MainView};

#[cfg(not(feature = "gui"))]
pub fn run_with_startup_error(startup_error: Option<String>) {
    if let Some(error) = startup_error {
        eprintln!("drift startup failed: {error}");
    } else {
        eprintln!("drift GUI disabled; rebuild with --features gui");
    }
}

#[cfg(not(feature = "gui"))]
pub fn run() {
    run_with_startup_error(None);
}
