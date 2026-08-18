#[cfg(feature = "gui")]
mod gui {
    use gpui::{
        div, prelude::*, App, Application, Context, IntoElement, Render, Window, WindowOptions,
    };

    #[derive(Default)]
    pub struct MainView;

    impl Render for MainView {
        fn render(&mut self, _window: &mut Window, _cx: &mut Context<Self>) -> impl IntoElement {
            div().size_full().child("drift")
        }
    }

    pub fn run() {
        Application::new().run(|cx: &mut App| {
            cx.open_window(WindowOptions::default(), |_, cx| cx.new(|_| MainView))
                .expect("failed to open drift window");
            cx.activate(true);
        });
    }
}

#[cfg(feature = "gui")]
pub use gui::{run, MainView};

#[cfg(not(feature = "gui"))]
pub fn run() {
    eprintln!("drift GUI disabled; rebuild with --features gui");
}
