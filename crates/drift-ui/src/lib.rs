mod send;
mod receive;

pub use receive::{
    ReceiveAction, ReceiveCommandError, ReceiveCommandErrorKind, ReceiveController, ReceiveEvent,
    ReceiveEventFuture, ReceiveEventStream, ReceiveFuture, ReceiveIntent, ReceivePhase,
    ReceiveViewState,
};
pub use send::{
    CopyFeedback, SelectedItem, SelectionError, SendAction, SendCommandError, SendCommandErrorKind,
    SendController, SendEvent, SendEventFuture, SendEventStream, SendFuture, SendIntent, SendPhase,
    SendProgress, SendSelection, SendViewState,
};

#[cfg(feature = "gui")]
mod gui {
    use gpui::{
        actions, div, prelude::*, AnyElement, App, Application, AsyncApp, ClickEvent,
        ClipboardItem, Context, FocusHandle, IntoElement, KeyDownEvent, KeyBinding, MouseButton,
        MouseDownEvent, PathPromptOptions, Render, SharedString, Task, WeakEntity, Window,
        WindowOptions,
    };
    use std::sync::Arc;

    use super::{
        ReceiveAction, ReceiveCommandError, ReceiveController, ReceiveEventStream, ReceiveIntent,
        ReceivePhase, ReceiveViewState, SendAction, SendCommandError, SendController,
        SendEventStream, SendIntent, SendPhase, SendViewState,
    };

    actions!(send, [ChooseFiles, StartSend, CopyTransferCode, CancelSend]);
    actions!(receive, [ChooseDestination, CheckCroc, StartReceive, CancelReceive]);
    actions!(navigation, [ShowSend, ShowReceive]);

    trait ClipboardService: Send + Sync {
        fn copy(&self, value: &str, cx: &mut App) -> Result<(), ()>;
    }

    struct GpuiClipboard;

    impl ClipboardService for GpuiClipboard {
        fn copy(&self, value: &str, cx: &mut App) -> Result<(), ()> {
            cx.write_to_clipboard(ClipboardItem::new_string(value.to_owned()));
            Ok(())
        }
    }

    struct UnavailableSendController;

    impl SendController for UnavailableSendController {
        fn preflight(
            &self,
            _paths: Vec<std::path::PathBuf>,
        ) -> super::SendFuture<Result<(), SendCommandError>> {
            Box::pin(async { Err(SendCommandError::preflight_failed()) })
        }

        fn start_send(
            &self,
            _paths: Vec<std::path::PathBuf>,
        ) -> super::SendFuture<Result<drift_core::TransferId, SendCommandError>> {
            Box::pin(async { Err(SendCommandError::start_failed()) })
        }

        fn cancel(
            &self,
            _transfer_id: drift_core::TransferId,
        ) -> super::SendFuture<Result<(), SendCommandError>> {
            Box::pin(async { Err(SendCommandError::cancel_failed()) })
        }

        fn subscribe(&self) -> Box<dyn SendEventStream> {
            Box::new(EmptySendEventStream)
        }
    }

    struct EmptySendEventStream;

    impl SendEventStream for EmptySendEventStream {
        fn next(&mut self) -> super::SendEventFuture<'_> {
            Box::pin(async { None })
        }
    }

    struct UnavailableReceiveController;

    impl ReceiveController for UnavailableReceiveController {
        fn validate_destination(
            &self,
            _path: std::path::PathBuf,
        ) -> super::ReceiveFuture<Result<(), ReceiveCommandError>> {
            Box::pin(async { Err(ReceiveCommandError::destination_unavailable()) })
        }

        fn preflight(&self) -> super::ReceiveFuture<Result<(), ReceiveCommandError>> {
            Box::pin(async { Err(ReceiveCommandError::preflight_failed()) })
        }

        fn start_receive(
            &self,
            _code: String,
            _destination: std::path::PathBuf,
        ) -> super::ReceiveFuture<Result<drift_core::TransferId, ReceiveCommandError>> {
            Box::pin(async { Err(ReceiveCommandError::start_failed()) })
        }

        fn cancel(
            &self,
            _transfer_id: drift_core::TransferId,
        ) -> super::ReceiveFuture<Result<(), ReceiveCommandError>> {
            Box::pin(async { Err(ReceiveCommandError::cancel_failed()) })
        }

        fn subscribe(&self) -> Box<dyn ReceiveEventStream> {
            Box::new(EmptyReceiveEventStream)
        }
    }

    struct EmptyReceiveEventStream;

    impl ReceiveEventStream for EmptyReceiveEventStream {
        fn next(&mut self) -> super::ReceiveEventFuture<'_> {
            Box::pin(async { None })
        }
    }

    #[derive(Clone, Copy, PartialEq, Eq)]
    enum MainRoute {
        Send,
        Receive,
    }

    pub struct MainView {
        startup_error: Option<String>,
        route: MainRoute,
        send: SendViewState,
        receive: ReceiveViewState,
        controller: Arc<dyn SendController>,
        receive_controller: Arc<dyn ReceiveController>,
        clipboard: Arc<dyn ClipboardService>,
        receive_focus: FocusHandle,
        _send_event_task: Task<()>,
        _receive_event_task: Task<()>,
        command_task: Option<Task<()>>,
    }

    impl MainView {
        fn new(
            startup_error: Option<String>,
            controller: Arc<dyn SendController>,
            receive_controller: Arc<dyn ReceiveController>,
            cx: &mut Context<Self>,
        ) -> Self {
            Self::new_with_clipboard(
                startup_error,
                controller,
                receive_controller,
                Arc::new(GpuiClipboard),
                cx,
            )
        }

        fn new_with_clipboard(
            startup_error: Option<String>,
            controller: Arc<dyn SendController>,
            receive_controller: Arc<dyn ReceiveController>,
            clipboard: Arc<dyn ClipboardService>,
            cx: &mut Context<Self>,
        ) -> Self {
            let mut send_event_stream = controller.subscribe();
            let send_event_task =
                cx.spawn(async move |this: WeakEntity<MainView>, cx: &mut AsyncApp| {
                    while let Some(event) = send_event_stream.next().await {
                        if this
                            .update(&mut *cx, |view, cx| {
                                view.send.apply_event(event);
                                cx.notify();
                            })
                            .is_err()
                        {
                            break;
                        }
                    }
                });
            let mut receive_event_stream = receive_controller.subscribe();
            let receive_event_task =
                cx.spawn(async move |this: WeakEntity<MainView>, cx: &mut AsyncApp| {
                    while let Some(event) = receive_event_stream.next().await {
                        if this
                            .update(&mut *cx, |view, cx| {
                                view.receive.apply_event(event);
                                cx.notify();
                            })
                            .is_err()
                        {
                            break;
                        }
                    }
                });
            let receive = ReceiveViewState::new(receive_controller.default_destination());
            let initial_validation = receive.destination_validation_intent();
            let initial_command_task = initial_validation.and_then(|intent| {
                let ReceiveIntent::ValidateDestination { generation, path } = intent else {
                    return None;
                };
                let receive_controller = Arc::clone(&receive_controller);
                Some(cx.spawn(
                    async move |this: WeakEntity<MainView>, cx: &mut AsyncApp| {
                        let result = receive_controller.validate_destination(path).await;
                        let _ = this.update(&mut *cx, |view, cx| {
                            match result {
                                Ok(()) => view.receive.mark_destination_valid(generation),
                                Err(error) => {
                                    view.receive.mark_destination_failed(generation, error)
                                }
                            }
                            cx.notify();
                        });
                    },
                ))
            });
            Self {
                startup_error,
                route: MainRoute::Send,
                send: SendViewState::new(),
                receive,
                controller,
                receive_controller,
                clipboard,
                receive_focus: cx.focus_handle(),
                _send_event_task: send_event_task,
                _receive_event_task: receive_event_task,
                command_task: initial_command_task,
            }
        }

        fn dispatch_action(&mut self, action: SendAction, cx: &mut Context<Self>) {
            let Some(intent) = self.send.handle_action(action) else {
                return;
            };
            cx.notify();
            self.run_intent(intent, cx);
        }

        fn run_intent(&mut self, intent: SendIntent, cx: &mut Context<Self>) {
            match intent {
                SendIntent::Choose => self.start_choose(cx),
                SendIntent::Preflight { generation, paths } => {
                    self.start_preflight(generation, paths, cx)
                }
                SendIntent::Start { paths } => self.start_transfer(paths, cx),
                SendIntent::CopyCode { code } => {
                    let result = self.clipboard.copy(&code, cx);
                    self.send.mark_copy_result(result);
                    cx.notify();
                }
                SendIntent::Cancel { transfer_id } => self.cancel_transfer(transfer_id, cx),
            }
        }

        fn dispatch_receive_action(&mut self, action: ReceiveAction, cx: &mut Context<Self>) {
            let Some(intent) = self.receive.handle_action(action) else {
                return;
            };
            cx.notify();
            self.run_receive_intent(intent, cx);
        }

        fn run_receive_intent(&mut self, intent: ReceiveIntent, cx: &mut Context<Self>) {
            match intent {
                ReceiveIntent::ChooseDestination => self.start_choose_destination(cx),
                ReceiveIntent::ValidateDestination { generation, path } => {
                    self.start_validate_destination(generation, path, cx)
                }
                ReceiveIntent::Preflight { generation } => {
                    self.start_receive_preflight(generation, cx)
                }
                ReceiveIntent::Start { code, destination } => {
                    self.start_receive_transfer(code, destination, cx)
                }
                ReceiveIntent::Cancel { transfer_id } => {
                    self.cancel_receive_transfer(transfer_id, cx)
                }
            }
        }

        fn start_choose_destination(&mut self, cx: &mut Context<Self>) {
            let picker = cx.prompt_for_paths(PathPromptOptions {
                files: false,
                directories: true,
                multiple: false,
                prompt: Some(SharedString::from("Choose receive folder")),
            });
            let controller = Arc::clone(&self.receive_controller);
            self.command_task = Some(cx.spawn(
                async move |this: WeakEntity<MainView>, cx: &mut AsyncApp| {
                    let selection = match picker.await {
                        Ok(Ok(Some(mut paths))) => paths.pop(),
                        Ok(Ok(None)) => None,
                        Ok(Err(_)) | Err(_) => {
                            let _ = this.update(&mut *cx, |view, cx| {
                                view.receive.mark_destination_selection_failed();
                                cx.notify();
                            });
                            return;
                        }
                    };
                    let Some(destination) = selection else {
                        return;
                    };
                    let intent = this
                        .update(&mut *cx, |view, cx| {
                            let intent = view.receive.set_destination(destination);
                            cx.notify();
                            intent
                        })
                        .ok();
                    let Some(ReceiveIntent::ValidateDestination { generation, path }) = intent
                    else {
                        return;
                    };
                    let result = controller.validate_destination(path).await;
                    let _ = this.update(&mut *cx, |view, cx| {
                        match result {
                            Ok(()) => view.receive.mark_destination_valid(generation),
                            Err(error) => {
                                view.receive.mark_destination_failed(generation, error)
                            }
                        }
                        cx.notify();
                    });
                },
            ));
        }

        fn start_validate_destination(
            &mut self,
            generation: u64,
            path: std::path::PathBuf,
            cx: &mut Context<Self>,
        ) {
            let controller = Arc::clone(&self.receive_controller);
            self.command_task = Some(cx.spawn(
                async move |this: WeakEntity<MainView>, cx: &mut AsyncApp| {
                    let result = controller.validate_destination(path).await;
                    let _ = this.update(&mut *cx, |view, cx| {
                        match result {
                            Ok(()) => view.receive.mark_destination_valid(generation),
                            Err(error) => {
                                view.receive.mark_destination_failed(generation, error)
                            }
                        }
                        cx.notify();
                    });
                },
            ));
        }

        fn start_receive_preflight(&mut self, generation: u64, cx: &mut Context<Self>) {
            let controller = Arc::clone(&self.receive_controller);
            self.command_task = Some(cx.spawn(
                async move |this: WeakEntity<MainView>, cx: &mut AsyncApp| {
                    let result = controller.preflight().await;
                    let _ = this.update(&mut *cx, |view, cx| {
                        if result.is_ok() {
                            view.receive.mark_preflight_succeeded(generation);
                        } else {
                            view.receive.mark_preflight_failed(generation);
                        }
                        cx.notify();
                    });
                },
            ));
        }

        fn start_receive_transfer(
            &mut self,
            code: String,
            destination: std::path::PathBuf,
            cx: &mut Context<Self>,
        ) {
            let controller = Arc::clone(&self.receive_controller);
            self.command_task = Some(cx.spawn(
                async move |this: WeakEntity<MainView>, cx: &mut AsyncApp| {
                    match controller.start_receive(code, destination).await {
                        Ok(transfer_id) => {
                            let _ = this.update(&mut *cx, |view, cx| {
                                view.receive.mark_start_succeeded(transfer_id);
                                cx.notify();
                            });
                        }
                        Err(_) => {
                            let _ = this.update(&mut *cx, |view, cx| {
                                view.receive.mark_start_failed();
                                cx.notify();
                            });
                        }
                    }
                },
            ));
        }

        fn cancel_receive_transfer(
            &mut self,
            transfer_id: drift_core::TransferId,
            cx: &mut Context<Self>,
        ) {
            let controller = Arc::clone(&self.receive_controller);
            self.command_task = Some(cx.spawn(
                async move |this: WeakEntity<MainView>, cx: &mut AsyncApp| {
                    if controller.cancel(transfer_id).await.is_err() {
                        let _ = this.update(&mut *cx, |view, cx| {
                            view.receive.mark_cancel_failed();
                            cx.notify();
                        });
                    }
                },
            ));
        }

        fn on_receive_key_down(
            &mut self,
            event: &KeyDownEvent,
            _window: &mut Window,
            cx: &mut Context<Self>,
        ) {
            if self.route != MainRoute::Receive || event.is_held {
                return;
            }
            if !self.receive.code_input_enabled() {
                return;
            }
            let keystroke = &event.keystroke;
            if keystroke.key == "enter" {
                let action = if self.receive.start_enabled() {
                    Some(ReceiveAction::Start)
                } else if self.receive.preflight_enabled() {
                    Some(ReceiveAction::Preflight)
                } else {
                    None
                };
                if let Some(action) = action {
                    self.dispatch_receive_action(action, cx);
                }
                return;
            }
            if keystroke.key == "backspace" {
                let mut code = self.receive.code().to_owned();
                code.pop();
                self.receive.set_code(code);
                cx.notify();
                return;
            }
            if keystroke.key == "v"
                && (keystroke.modifiers.platform || keystroke.modifiers.control)
            {
                if let Some(text) = cx.read_from_clipboard().and_then(|item| item.text()) {
                    let mut code = self.receive.code().to_owned();
                    code.push_str(&text.replace('\n', "").replace('\r', ""));
                    self.receive.set_code(code);
                    cx.notify();
                }
                return;
            }
            if keystroke.modifiers.control
                || keystroke.modifiers.alt
                || keystroke.modifiers.platform
                || keystroke.modifiers.function
            {
                return;
            }
            if let Some(input) = keystroke.key_char.as_ref() {
                let mut code = self.receive.code().to_owned();
                code.push_str(input);
                self.receive.set_code(code);
                cx.notify();
            }
        }

        fn focus_receive_input(
            &mut self,
            _: &MouseDownEvent,
            window: &mut Window,
            _cx: &mut Context<Self>,
        ) {
            window.focus(&self.receive_focus);
        }

        fn start_choose(&mut self, cx: &mut Context<Self>) {
            let controller = Arc::clone(&self.controller);
            self.command_task = Some(cx.spawn(
                async move |this: WeakEntity<MainView>, cx: &mut AsyncApp| {
                    match controller.choose().await {
                        Ok(selection) => {
                            let Some(SendIntent::Preflight { generation, paths }) = this
                                .update(&mut *cx, |view, cx| {
                                    let intent = view.send.set_selection(selection);
                                    cx.notify();
                                    intent
                                })
                                .ok()
                            else {
                                return;
                            };
                            match controller.preflight(paths).await {
                                Ok(()) => {
                                    let _ = this.update(&mut *cx, |view, cx| {
                                        view.send.mark_preflight_succeeded(generation);
                                        cx.notify();
                                    });
                                }
                                Err(_) => {
                                    let _ = this.update(&mut *cx, |view, cx| {
                                        view.send.mark_preflight_failed(generation);
                                        cx.notify();
                                    });
                                }
                            }
                        }
                        Err(_) => {
                            let _ = this.update(&mut *cx, |view, cx| {
                                view.send.mark_choose_failed();
                                cx.notify();
                            });
                        }
                    }
                },
            ));
        }

        fn start_preflight(
            &mut self,
            generation: u64,
            paths: Vec<std::path::PathBuf>,
            cx: &mut Context<Self>,
        ) {
            let controller = Arc::clone(&self.controller);
            self.command_task = Some(cx.spawn(
                async move |this: WeakEntity<MainView>, cx: &mut AsyncApp| {
                    let result = controller.preflight(paths).await;
                    let _ = this.update(&mut *cx, |view, cx| {
                        if result.is_ok() {
                            view.send.mark_preflight_succeeded(generation);
                        } else {
                            view.send.mark_preflight_failed(generation);
                        }
                        cx.notify();
                    });
                },
            ));
        }

        fn start_transfer(&mut self, paths: Vec<std::path::PathBuf>, cx: &mut Context<Self>) {
            let controller = Arc::clone(&self.controller);
            self.command_task = Some(cx.spawn(
                async move |this: WeakEntity<MainView>, cx: &mut AsyncApp| {
                    match controller.start_send(paths).await {
                        Ok(transfer_id) => {
                            let _ = this.update(&mut *cx, |view, cx| {
                                view.send.mark_start_succeeded(transfer_id);
                                cx.notify();
                            });
                        }
                        Err(_) => {
                            let _ = this.update(&mut *cx, |view, cx| {
                                view.send.mark_start_failed();
                                cx.notify();
                            });
                        }
                    }
                },
            ));
        }

        fn cancel_transfer(&mut self, transfer_id: drift_core::TransferId, cx: &mut Context<Self>) {
            let controller = Arc::clone(&self.controller);
            self.command_task = Some(cx.spawn(
                async move |this: WeakEntity<MainView>, cx: &mut AsyncApp| {
                    if controller.cancel(transfer_id).await.is_err() {
                        let _ = this.update(&mut *cx, |view, cx| {
                            view.send.mark_cancel_failed();
                            cx.notify();
                        });
                    }
                },
            ));
        }

        fn render_error(&self) -> AnyElement {
            div()
                .size_full()
                .p_8()
                .bg(gpui::rgb(0xf7f4ee))
                .text_color(gpui::rgb(0x42251f))
                .child(self.startup_error.clone().unwrap_or_default())
                .into_any_element()
        }

        fn render_send(&mut self, cx: &mut Context<Self>) -> AnyElement {
            let phase = self.send.phase();
            let selection = self.send.selection();
            let summary = selection.map(|selection| {
                format!(
                    "{} item(s) / {}",
                    selection.item_count(),
                    format_bytes(selection.total_bytes())
                )
            });
            let mut selection_list = div().flex().flex_col().gap_2();
            if let Some(selection) = selection {
                for (index, item) in selection.items().iter().enumerate() {
                    let label = item
                        .path()
                        .file_name()
                        .and_then(|name| name.to_str())
                        .map_or_else(|| "Selected item".to_owned(), ToOwned::to_owned);
                    let remove = cx.listener(move |view: &mut MainView, _: &ClickEvent, _, cx| {
                        view.dispatch_action(SendAction::RemoveSelection { index }, cx);
                    });
                    selection_list = selection_list.child(
                        div()
                            .id(SharedString::from(format!("send-item-{index}")))
                            .flex()
                            .items_center()
                            .justify_between()
                            .gap_2()
                            .child(label)
                            .child(action_button(
                                SharedString::from(format!("send-remove-{index}")),
                                "Remove",
                                true,
                                remove,
                            )),
                    );
                }
            }

            let choose = cx.listener(|view: &mut MainView, _: &ClickEvent, _, cx| {
                view.dispatch_action(SendAction::Choose, cx);
            });
            let start = cx.listener(|view: &mut MainView, _: &ClickEvent, _, cx| {
                view.dispatch_action(SendAction::Start, cx);
            });
            let copy = cx.listener(|view: &mut MainView, _: &ClickEvent, _, cx| {
                view.dispatch_action(SendAction::CopyCode, cx);
            });
            let cancel = cx.listener(|view: &mut MainView, _: &ClickEvent, _, cx| {
                view.dispatch_action(SendAction::Cancel, cx);
            });

            let mut code_panel = div().flex().items_center().gap_2();
            if let Some(code) = self.send.transfer_code() {
                code_panel = code_panel
                    .child(div().flex_1().child(code.to_owned()))
                    .child(action_button("send-copy-code", "Copy code", true, copy));
            }

            let mut root = div()
                .id("send-view")
                .key_context("SendView")
                .on_action(cx.listener(|view: &mut MainView, _: &ChooseFiles, _, cx| {
                    view.dispatch_action(SendAction::Choose, cx);
                }))
                .on_action(cx.listener(|view: &mut MainView, _: &StartSend, _, cx| {
                    view.dispatch_action(SendAction::Start, cx);
                }))
                .on_action(
                    cx.listener(|view: &mut MainView, _: &CopyTransferCode, _, cx| {
                        view.dispatch_action(SendAction::CopyCode, cx);
                    }),
                )
                .on_action(cx.listener(|view: &mut MainView, _: &CancelSend, _, cx| {
                    view.dispatch_action(SendAction::Cancel, cx);
                }))
                .size_full()
                .p_8()
                .flex()
                .flex_col()
                .gap_4()
                .bg(gpui::rgb(0xf7f4ee))
                .text_color(gpui::rgb(0x1d2a24))
                .on_action(cx.listener(|view: &mut MainView, _: &ShowReceive, _, cx| {
                    view.route = MainRoute::Receive;
                    cx.notify();
                }))
                .child(div().child("Send"))
                .child(self.render_navigation(cx))
                .child(div().child(phase.label()))
                .child(
                    div()
                        .id("send-selection")
                        .p_4()
                        .flex()
                        .flex_col()
                        .gap_2()
                        .bg(gpui::rgb(0xffffff))
                        .border_1()
                        .border_color(gpui::rgb(0xd7d0c4))
                        .rounded_sm()
                        .when(summary.is_none(), |this| {
                            this.child("Choose files or folders to begin.")
                        })
                        .when(summary.is_some(), |this| {
                            this.child(summary.clone().unwrap_or_default())
                                .child(selection_list)
                        }),
                )
                .child(code_panel)
                .child(
                    div()
                        .flex()
                        .items_center()
                        .gap_2()
                        .child(action_button(
                            "send-choose",
                            "Choose files",
                            self.send.choose_enabled(),
                            choose,
                        ))
                        .child(action_button(
                            "send-start",
                            if phase == SendPhase::Failed {
                                "Retry"
                            } else {
                                "Start transfer"
                            },
                            self.send.start_enabled(),
                            start,
                        ))
                        .child(action_button(
                            "send-cancel",
                            "Cancel",
                            self.send.cancel_enabled(),
                            cancel,
                        )),
                );

            if let Some(progress) = self.send.progress() {
                let progress_label = if self.send.progress_available() {
                    format!(
                        "{} / {}",
                        format_bytes(progress.transferred),
                        format_bytes(progress.total)
                    )
                } else {
                    "Progress unavailable".to_owned()
                };
                root = root.child(div().child(progress_label));
            } else if !self.send.progress_available() {
                root = root.child(div().child("Progress unavailable"));
            }
            if let Some(feedback) = self.send.copy_feedback() {
                root = root.child(div().child(match feedback {
                    super::CopyFeedback::Succeeded => "Code copied",
                    super::CopyFeedback::Failed => "Copy failed",
                }));
            }
            if let Some(error) = self.send.error() {
                root = root.child(
                    div()
                        .text_color(gpui::rgb(0x9a3025))
                        .child(error.to_owned()),
                );
            }
            root.into_any_element()
        }

        fn render_receive(&mut self, cx: &mut Context<Self>) -> AnyElement {
            let phase = self.receive.phase();
            let choose = cx.listener(|view: &mut MainView, _: &ClickEvent, _, cx| {
                view.dispatch_receive_action(ReceiveAction::ChooseDestination, cx);
            });
            let preflight = cx.listener(|view: &mut MainView, _: &ClickEvent, _, cx| {
                view.dispatch_receive_action(ReceiveAction::Preflight, cx);
            });
            let start = cx.listener(|view: &mut MainView, _: &ClickEvent, _, cx| {
                view.dispatch_receive_action(ReceiveAction::Start, cx);
            });
            let cancel = cx.listener(|view: &mut MainView, _: &ClickEvent, _, cx| {
                view.dispatch_receive_action(ReceiveAction::Cancel, cx);
            });
            let code_field = if self.receive.code().is_empty() {
                "Paste transfer code".to_owned()
            } else {
                self.receive.code().to_owned()
            };
            let destination = self
                .receive
                .destination()
                .map(|path| path.to_string_lossy().into_owned())
                .unwrap_or_else(|| "Choose a receive folder".to_owned());
            let mut root = div()
                .id("receive-view")
                .key_context("ReceiveView")
                .on_action(cx.listener(|view: &mut MainView, _: &ShowSend, _, cx| {
                    view.route = MainRoute::Send;
                    cx.notify();
                }))
                .on_action(cx.listener(|view: &mut MainView, _: &ChooseDestination, _, cx| {
                    view.dispatch_receive_action(ReceiveAction::ChooseDestination, cx);
                }))
                .on_action(cx.listener(|view: &mut MainView, _: &CheckCroc, _, cx| {
                    view.dispatch_receive_action(ReceiveAction::Preflight, cx);
                }))
                .on_action(cx.listener(|view: &mut MainView, _: &StartReceive, _, cx| {
                    view.dispatch_receive_action(ReceiveAction::Start, cx);
                }))
                .on_action(cx.listener(|view: &mut MainView, _: &CancelReceive, _, cx| {
                    view.dispatch_receive_action(ReceiveAction::Cancel, cx);
                }))
                .size_full()
                .p_8()
                .flex()
                .flex_col()
                .gap_4()
                .bg(gpui::rgb(0xf7f4ee))
                .text_color(gpui::rgb(0x1d2a24))
                .child(div().child("Receive"))
                .child(self.render_navigation(cx))
                .child(div().child(phase.label()))
                .child(
                    div()
                        .id("receive-code-input")
                        .track_focus(&self.receive_focus)
                        .on_mouse_down(
                            MouseButton::Left,
                            cx.listener(|view: &mut MainView, event, window, cx| {
                                view.focus_receive_input(event, window, cx);
                            }),
                        )
                        .on_key_down(cx.listener(|view: &mut MainView, event, window, cx| {
                            view.on_receive_key_down(event, window, cx);
                        }))
                        .p_4()
                        .border_1()
                        .border_color(gpui::rgb(0xd7d0c4))
                        .rounded_sm()
                        .bg(gpui::rgb(0xffffff))
                        .child(code_field),
                )
                .child(
                    div()
                        .id("receive-destination")
                        .p_4()
                        .flex()
                        .items_center()
                        .justify_between()
                        .gap_2()
                        .border_1()
                        .border_color(gpui::rgb(0xd7d0c4))
                        .rounded_sm()
                        .bg(gpui::rgb(0xffffff))
                        .child(destination)
                        .child(action_button(
                            "receive-choose-destination",
                            "Choose folder",
                            self.receive.choose_destination_enabled(),
                            choose,
                        )),
                )
                .child(
                    div()
                        .flex()
                        .items_center()
                        .gap_2()
                        .child(action_button(
                            "receive-preflight",
                            if phase == ReceivePhase::Failed {
                                "Retry check"
                            } else {
                                "Check Croc"
                            },
                            self.receive.preflight_enabled(),
                            preflight,
                        ))
                        .child(action_button(
                            "receive-start",
                            if phase == ReceivePhase::Failed {
                                "Retry receive"
                            } else {
                                "Receive"
                            },
                            self.receive.start_enabled(),
                            start,
                        ))
                        .child(action_button(
                            "receive-cancel",
                            "Cancel",
                            self.receive.cancel_enabled(),
                            cancel,
                        )),
                );

            if let Some(progress) = self.receive.progress() {
                let progress_label = if self.receive.progress_available() {
                    format!(
                        "{} / {}",
                        format_bytes(progress.0),
                        format_bytes(progress.1)
                    )
                } else {
                    "Progress unavailable".to_owned()
                };
                root = root.child(div().child(progress_label));
            } else if !self.receive.progress_available() {
                root = root.child(div().child("Progress unavailable"));
            }
            if let Some(error) = self.receive.code_error() {
                root = root.child(div().text_color(gpui::rgb(0x9a3025)).child(error.to_owned()));
            }
            if let Some(error) = self.receive.destination_error() {
                root = root.child(div().text_color(gpui::rgb(0x9a3025)).child(error.to_owned()));
            }
            if let Some(error) = self.receive.error() {
                root = root.child(div().text_color(gpui::rgb(0x9a3025)).child(error.to_owned()));
            }
            root.into_any_element()
        }

        fn render_navigation(&mut self, cx: &mut Context<Self>) -> AnyElement {
            let send = cx.listener(|view: &mut MainView, _: &ClickEvent, _, cx| {
                view.route = MainRoute::Send;
                cx.notify();
            });
            let receive = cx.listener(|view: &mut MainView, _: &ClickEvent, _, cx| {
                view.route = MainRoute::Receive;
                cx.notify();
            });
            div()
                .flex()
                .items_center()
                .gap_2()
                .child(action_button("route-send", "Send", self.route == MainRoute::Send, send))
                .child(action_button(
                    "route-receive",
                    "Receive",
                    self.route == MainRoute::Receive,
                    receive,
                ))
                .into_any_element()
        }
    }

    impl Render for MainView {
        fn render(&mut self, _window: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
            if self.startup_error.is_some() {
                self.render_error()
            } else {
                match self.route {
                    MainRoute::Send => self.render_send(cx),
                    MainRoute::Receive => self.render_receive(cx),
                }
            }
        }
    }

    fn action_button(
        id: impl Into<SharedString>,
        label: impl Into<SharedString>,
        enabled: bool,
        on_click: impl Fn(&ClickEvent, &mut Window, &mut App) + 'static,
    ) -> impl IntoElement {
        let mut button = div()
            .id(id.into())
            .flex_none()
            .px_4()
            .py_2()
            .rounded_sm()
            .border_1()
            .border_color(gpui::rgb(0xd7d0c4))
            .child(label.into());
        if enabled {
            button = button
                .bg(gpui::rgb(0x235347))
                .text_color(gpui::rgb(0xffffff))
                .cursor_pointer()
                .on_click(on_click);
        } else {
            button = button
                .bg(gpui::rgb(0xe3ded5))
                .text_color(gpui::rgb(0x817a70));
        }
        button
    }

    fn format_bytes(bytes: u64) -> String {
        const KIB: u64 = 1024;
        const MIB: u64 = KIB * 1024;
        const GIB: u64 = MIB * 1024;
        if bytes >= GIB {
            format!("{:.1} GiB", bytes as f64 / GIB as f64)
        } else if bytes >= MIB {
            format!("{:.1} MiB", bytes as f64 / MIB as f64)
        } else if bytes >= KIB {
            format!("{:.1} KiB", bytes as f64 / KIB as f64)
        } else {
            format!("{bytes} B")
        }
    }

    pub fn run_with_controller(controller: Arc<dyn SendController>) {
        run_with_controllers(controller, Arc::new(UnavailableReceiveController));
    }

    pub fn run_with_controllers(
        controller: Arc<dyn SendController>,
        receive_controller: Arc<dyn ReceiveController>,
    ) {
        run_with_startup_error_and_controllers(None, controller, receive_controller);
    }

    pub fn run_with_startup_error(startup_error: Option<String>) {
        run_with_startup_error_and_controllers(
            startup_error,
            Arc::new(UnavailableSendController),
            Arc::new(UnavailableReceiveController),
        );
    }

    fn run_with_startup_error_and_controllers(
        startup_error: Option<String>,
        controller: Arc<dyn SendController>,
        receive_controller: Arc<dyn ReceiveController>,
    ) {
        Application::new().run(move |cx: &mut App| {
            cx.bind_keys([
                KeyBinding::new("cmd-o", ChooseFiles, Some("SendView")),
                KeyBinding::new("cmd-enter", StartSend, Some("SendView")),
                KeyBinding::new("cmd-c", CopyTransferCode, Some("SendView")),
                KeyBinding::new("escape", CancelSend, Some("SendView")),
                KeyBinding::new("cmd-shift-o", ChooseDestination, Some("ReceiveView")),
                KeyBinding::new("cmd-enter", StartReceive, Some("ReceiveView")),
                KeyBinding::new("escape", CancelReceive, Some("ReceiveView")),
            ]);
            cx.open_window(WindowOptions::default(), move |_, cx| {
                cx.new(|cx| {
                    MainView::new(
                        startup_error,
                        controller,
                        receive_controller,
                        cx,
                    )
                })
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
pub use gui::{
    run, run_with_controller, run_with_controllers, run_with_startup_error, MainView,
};

#[cfg(not(feature = "gui"))]
pub fn run_with_startup_error(startup_error: Option<String>) {
    if let Some(error) = startup_error {
        eprintln!("drift startup failed: {error}");
    } else {
        eprintln!("drift GUI disabled; rebuild with --features gui");
    }
}

#[cfg(not(feature = "gui"))]
pub fn run_with_controller(_controller: std::sync::Arc<dyn SendController>) {
    eprintln!("drift GUI disabled; rebuild with --features gui");
}

#[cfg(not(feature = "gui"))]
pub fn run_with_controllers(
    _controller: std::sync::Arc<dyn SendController>,
    _receive_controller: std::sync::Arc<dyn ReceiveController>,
) {
    eprintln!("drift GUI disabled; rebuild with --features gui");
}

#[cfg(not(feature = "gui"))]
pub fn run() {
    run_with_startup_error(None);
}
