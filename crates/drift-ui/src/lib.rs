mod send;
mod receive;
mod progress;
mod settings;
mod transfer;

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
pub use settings::{
    RelaySettingsSnapshot, SettingsAction, SettingsCommandError, SettingsCommandErrorKind,
    SettingsController, SettingsFuture, SettingsIntent, SettingsPhase, SettingsViewState,
};
pub use transfer::{
    failure_label, RelayStatus, TransferAction, TransferCommandError, TransferCommandErrorKind,
    TransferCommandFuture, TransferController, TransferDetail, TransferDirection,
    TransferEventFuture, TransferEventStream, TransferListModel, TransferListState,
    TransferListFuture, TransferSnapshot, TransferSummary, TransferControls,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RecoveryKind {
    Send,
    Receive,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RecoveryCandidate {
    pub transfer_id: drift_core::TransferId,
    pub kind: RecoveryKind,
}

#[cfg(feature = "gui")]
mod gui {
    use gpui::{
        actions, div, prelude::*, AnyElement, App, Application, AsyncApp, ClickEvent,
        ClipboardItem, Context, ExternalPaths, FocusHandle, IntoElement, KeyDownEvent, KeyBinding,
        MouseButton, MouseDownEvent, PathPromptOptions, Render, SharedString, Task, WeakEntity,
        Window, WindowOptions,
    };
    use std::sync::Arc;
    use std::{future::Future, path::PathBuf, pin::Pin};

    use super::{
        ReceiveAction, ReceiveCommandError, ReceiveController, ReceiveEventStream, ReceiveIntent,
        ReceivePhase, ReceiveViewState, RecoveryCandidate, RecoveryKind, SendAction,
        SendCommandError, SendController, SendEventStream, SendIntent, SendPhase, SendViewState,
        RelaySettingsSnapshot, SettingsAction, SettingsCommandError, SettingsCommandErrorKind,
        SettingsController, SettingsFuture, SettingsIntent, SettingsViewState, TransferAction,
        TransferCommandError, TransferCommandErrorKind, TransferController, TransferEventStream,
        TransferListModel,
    };

    actions!(
        send,
        [ChooseFiles, StartSend, CopyTransferCode, CancelSend, RecoverSend, DiscardSendRecovery]
    );
    actions!(
        receive,
        [
            ChooseDestination,
            CheckCroc,
            StartReceive,
            CancelReceive,
            RecoverReceive,
            DiscardReceiveRecovery
        ]
    );
    actions!(
        navigation,
        [ShowHome, ShowSend, ShowReceive, ShowTransfers, ShowSettings]
    );

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

    type PathPickerFuture =
        Pin<Box<dyn Future<Output = Result<Option<Vec<PathBuf>>, ()>> + Send + 'static>>;

    trait SendPathPicker: Send + Sync {
        fn choose(&self, cx: &mut App) -> PathPickerFuture;
    }

    struct GpuiSendPathPicker;

    impl SendPathPicker for GpuiSendPathPicker {
        fn choose(&self, cx: &mut App) -> PathPickerFuture {
            let picker = cx.prompt_for_paths(PathPromptOptions {
                files: true,
                directories: true,
                multiple: true,
                prompt: Some(SharedString::from("Choose files or folders")),
            });
            Box::pin(async move { picker.await.map_err(|_| ())?.map_err(|_| ()) })
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
            _manifest: Option<drift_core::TransferManifest>,
        ) -> super::SendFuture<Result<drift_core::TransferId, SendCommandError>> {
            Box::pin(async { Err(SendCommandError::start_failed()) })
        }

        fn cancel(
            &self,
            _transfer_id: drift_core::TransferId,
        ) -> super::SendFuture<Result<(), SendCommandError>> {
            Box::pin(async { Err(SendCommandError::cancel_failed()) })
        }

        fn retry(
            &self,
            _transfer_id: drift_core::TransferId,
        ) -> super::SendFuture<Result<drift_core::TransferId, SendCommandError>> {
            Box::pin(async { Err(SendCommandError::start_failed()) })
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

        fn retry(
            &self,
            _transfer_id: drift_core::TransferId,
        ) -> super::ReceiveFuture<Result<drift_core::TransferId, ReceiveCommandError>> {
            Box::pin(async { Err(ReceiveCommandError::start_failed()) })
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

    struct UnavailableTransferController;

    impl TransferController for UnavailableTransferController {
        fn cancel(
            &self,
            _transfer_id: drift_core::TransferId,
        ) -> super::TransferCommandFuture {
            Box::pin(async {
                Err(TransferCommandError::new(
                    TransferCommandErrorKind::Unavailable,
                ))
            })
        }

        fn retry(
            &self,
            transfer_id: drift_core::TransferId,
        ) -> super::TransferCommandFuture {
            self.cancel(transfer_id)
        }

        fn pause(
            &self,
            transfer_id: drift_core::TransferId,
        ) -> super::TransferCommandFuture {
            self.cancel(transfer_id)
        }

        fn resume(
            &self,
            transfer_id: drift_core::TransferId,
        ) -> super::TransferCommandFuture {
            self.cancel(transfer_id)
        }

        fn reveal_destination(
            &self,
            transfer_id: drift_core::TransferId,
        ) -> super::TransferCommandFuture {
            self.cancel(transfer_id)
        }

        fn subscribe(&self) -> Box<dyn TransferEventStream> {
            Box::new(EmptyTransferEventStream)
        }
    }

    struct EmptyTransferEventStream;

    impl TransferEventStream for EmptyTransferEventStream {
        fn next(&mut self) -> super::TransferEventFuture<'_> {
            Box::pin(async { None })
        }
    }

    struct UnavailableSettingsController;

    impl SettingsController for UnavailableSettingsController {
        fn load(&self) -> SettingsFuture<Result<RelaySettingsSnapshot, SettingsCommandError>> {
            Box::pin(async {
                Err(SettingsCommandError::new(
                    SettingsCommandErrorKind::LoadFailed,
                ))
            })
        }

        fn save(
            &self,
            _enabled: bool,
            _endpoint: Option<String>,
        ) -> SettingsFuture<Result<RelaySettingsSnapshot, SettingsCommandError>> {
            Box::pin(async {
                Err(SettingsCommandError::new(
                    SettingsCommandErrorKind::SaveFailed,
                ))
            })
        }

        fn clear(&self) -> SettingsFuture<Result<RelaySettingsSnapshot, SettingsCommandError>> {
            Box::pin(async {
                Err(SettingsCommandError::new(
                    SettingsCommandErrorKind::SaveFailed,
                ))
            })
        }
    }

    #[derive(Clone, Copy, PartialEq, Eq)]
    enum MainRoute {
        Home,
        Send,
        Receive,
        Transfers,
        Settings,
    }

    pub struct MainView {
        startup_error: Option<String>,
        route: MainRoute,
        send: SendViewState,
        receive: ReceiveViewState,
        settings: SettingsViewState,
        transfers: TransferListModel,
        controller: Arc<dyn SendController>,
        receive_controller: Arc<dyn ReceiveController>,
        settings_controller: Arc<dyn SettingsController>,
        transfer_controller: Arc<dyn TransferController>,
        clipboard: Arc<dyn ClipboardService>,
        path_picker: Arc<dyn SendPathPicker>,
        receive_focus: FocusHandle,
        settings_focus: FocusHandle,
        _send_event_task: Task<()>,
        _receive_event_task: Task<()>,
        _transfer_event_task: Task<()>,
        _transfer_load_task: Task<()>,
        _recovery_task: Task<()>,
        _settings_task: Task<()>,
        recovery_candidates: Vec<RecoveryCandidate>,
        command_task: Option<Task<()>>,
        transfer_command_error: Option<String>,
        pending_transfer_selection: Option<drift_core::TransferId>,
    }

    impl MainView {
        fn new(
            startup_error: Option<String>,
            controller: Arc<dyn SendController>,
            receive_controller: Arc<dyn ReceiveController>,
            settings_controller: Arc<dyn SettingsController>,
            transfer_controller: Arc<dyn TransferController>,
            cx: &mut Context<Self>,
        ) -> Self {
            Self::new_with_clipboard(
                startup_error,
                controller,
                receive_controller,
                settings_controller,
                transfer_controller,
                Arc::new(GpuiClipboard),
                cx,
            )
        }

        fn new_with_clipboard(
            startup_error: Option<String>,
            controller: Arc<dyn SendController>,
            receive_controller: Arc<dyn ReceiveController>,
            settings_controller: Arc<dyn SettingsController>,
            transfer_controller: Arc<dyn TransferController>,
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
            let mut transfer_event_stream = transfer_controller.subscribe();
            let transfer_event_task =
                cx.spawn(async move |this: WeakEntity<MainView>, cx: &mut AsyncApp| {
                    while let Some(snapshot) = transfer_event_stream.next().await {
                        if this
                            .update(&mut *cx, |view, cx| {
                                let transfer_id = snapshot.transfer_id;
                                view.transfers.upsert(snapshot);
                                if view.pending_transfer_selection == Some(transfer_id) {
                                    view.transfers.select(Some(transfer_id));
                                    view.pending_transfer_selection = None;
                                }
                                cx.notify();
                            })
                            .is_err()
                        {
                            break;
                        }
                    }
                });
            let transfer_controller_for_load = Arc::clone(&transfer_controller);
            let transfer_load_task = cx.spawn(
                async move |this: WeakEntity<MainView>, cx: &mut AsyncApp| {
                    let result = transfer_controller_for_load.load().await;
                    let _ = this.update(&mut *cx, |view, cx| {
                        match result {
                            Ok(snapshots) => {
                                view.transfers.set_loading(false);
                                for snapshot in snapshots {
                                    view.transfers.upsert(snapshot);
                                }
                            }
                            Err(error) => view.transfers.set_error(Some(error.message())),
                        }
                        cx.notify();
                    });
                },
            );
            let receive = ReceiveViewState::new(receive_controller.default_destination());
            let recovery_controller = Arc::clone(&controller);
            let recovery_task = cx.spawn(
                async move |this: WeakEntity<MainView>, cx: &mut AsyncApp| {
                    let candidates = recovery_controller
                        .recoveries()
                        .await
                        .unwrap_or_default();
                    let _ = this.update(&mut *cx, |view, cx| {
                        view.recovery_candidates = candidates;
                        view.transfers.replace_recoveries(&view.recovery_candidates);
                        cx.notify();
                    });
                },
            );
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
            let settings_controller_for_load = Arc::clone(&settings_controller);
            let settings_task = cx.spawn(
                async move |this: WeakEntity<MainView>, cx: &mut AsyncApp| {
                    let result = settings_controller_for_load.load().await;
                    let _ = this.update(&mut *cx, |view, cx| {
                        match result {
                            Ok(snapshot) => view.settings.apply_loaded(snapshot),
                            Err(error) => view.settings.mark_failed(error),
                        }
                        cx.notify();
                    });
                },
            );
            let mut transfers = TransferListModel::default();
            transfers.set_loading(true);
            Self {
                startup_error,
                route: MainRoute::Home,
                send: SendViewState::new(),
                receive,
                settings: SettingsViewState::new(),
                transfers,
                controller,
                receive_controller,
                settings_controller,
                transfer_controller,
                clipboard,
                path_picker: Arc::new(GpuiSendPathPicker),
                receive_focus: cx.focus_handle(),
                settings_focus: cx.focus_handle(),
                _send_event_task: send_event_task,
                _receive_event_task: receive_event_task,
                _transfer_event_task: transfer_event_task,
                _transfer_load_task: transfer_load_task,
                _recovery_task: recovery_task,
                _settings_task: settings_task,
                recovery_candidates: Vec::new(),
                command_task: initial_command_task,
                transfer_command_error: None,
                pending_transfer_selection: None,
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
                SendIntent::CancelScan => self.controller.cancel_scan(),
                SendIntent::Preflight { generation, paths } => {
                    self.start_preflight(generation, paths, cx)
                }
                SendIntent::Start { paths, manifest } => self.start_transfer(paths, manifest, cx),
                SendIntent::Retry { transfer_id } => self.retry_transfer(transfer_id, cx),
                SendIntent::Recover { transfer_id } => self.recover_send(transfer_id, cx),
                SendIntent::DiscardRecovery { transfer_id } => {
                    self.discard_send_recovery(transfer_id, cx)
                }
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

        fn dispatch_transfer_action(&mut self, action: TransferAction, cx: &mut Context<Self>) {
            let Some(transfer_id) = self.transfers.selected() else {
                return;
            };
            let Some(detail) = self.transfers.selected_detail() else {
                return;
            };
            let enabled = match action {
                TransferAction::Cancel => detail.summary.controls.cancel,
                TransferAction::Retry => detail.summary.controls.retry,
                TransferAction::Pause => detail.summary.controls.pause,
                TransferAction::Resume => detail.summary.controls.resume,
                TransferAction::RevealDestination => detail.summary.controls.reveal_destination,
            };
            if !enabled {
                return;
            }
            self.transfer_command_error = None;
            let controller = Arc::clone(&self.transfer_controller);
            self.command_task = Some(cx.spawn(
                async move |this: WeakEntity<MainView>, cx: &mut AsyncApp| {
                    let result = match action {
                        TransferAction::Cancel => controller.cancel(transfer_id).await,
                        TransferAction::Retry => controller.retry(transfer_id).await,
                        TransferAction::Pause => controller.pause(transfer_id).await,
                        TransferAction::Resume => controller.resume(transfer_id).await,
                        TransferAction::RevealDestination => {
                            controller.reveal_destination(transfer_id).await
                        }
                    };
                    let _ = this.update(&mut *cx, |view, cx| {
                        match result {
                            Ok(new_transfer_id) if action == TransferAction::Retry => {
                                view.transfers.remove(transfer_id);
                                if view
                                    .transfers
                                    .rows()
                                    .iter()
                                    .any(|row| row.transfer_id == new_transfer_id)
                                {
                                    view.transfers.select(Some(new_transfer_id));
                                } else {
                                    view.pending_transfer_selection = Some(new_transfer_id);
                                }
                            }
                            Ok(_) => {}
                            Err(error) => {
                                view.transfer_command_error = Some(error.message().to_owned());
                            }
                        }
                        cx.notify();
                    });
                },
            ));
            cx.notify();
        }

        fn remove_recovery_candidate(&mut self, transfer_id: drift_core::TransferId) {
            self.recovery_candidates
                .retain(|candidate| candidate.transfer_id != transfer_id);
            self.transfers.remove_recovery(transfer_id);
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
                ReceiveIntent::Retry { transfer_id } => {
                    self.retry_receive_transfer(transfer_id, cx)
                }
                ReceiveIntent::Recover {
                    transfer_id,
                    code,
                    destination,
                } => self.recover_receive(transfer_id, code, destination, cx),
                ReceiveIntent::DiscardRecovery { transfer_id } => {
                    self.discard_receive_recovery(transfer_id, cx)
                }
                ReceiveIntent::Cancel { transfer_id } => {
                    self.cancel_receive_transfer(transfer_id, cx)
                }
            }
        }

        fn dispatch_settings_action(&mut self, action: SettingsAction, cx: &mut Context<Self>) {
            let Some(intent) = self.settings.handle_action(action) else {
                return;
            };
            cx.notify();
            self.run_settings_intent(intent, cx);
        }

        fn run_settings_intent(&mut self, intent: SettingsIntent, cx: &mut Context<Self>) {
            match intent {
                SettingsIntent::Save { enabled, endpoint } => {
                    self.save_settings(enabled, endpoint, cx)
                }
                SettingsIntent::Clear => self.clear_settings(cx),
            }
        }

        fn save_settings(
            &mut self,
            enabled: bool,
            endpoint: Option<String>,
            cx: &mut Context<Self>,
        ) {
            let controller = Arc::clone(&self.settings_controller);
            self.command_task = Some(cx.spawn(
                async move |this: WeakEntity<MainView>, cx: &mut AsyncApp| {
                    let result = controller.save(enabled, endpoint).await;
                    let _ = this.update(&mut *cx, |view, cx| {
                        match result {
                            Ok(snapshot) => view.settings.mark_saved(snapshot),
                            Err(error) => view.settings.mark_failed(error),
                        }
                        cx.notify();
                    });
                },
            ));
        }

        fn clear_settings(&mut self, cx: &mut Context<Self>) {
            let controller = Arc::clone(&self.settings_controller);
            self.command_task = Some(cx.spawn(
                async move |this: WeakEntity<MainView>, cx: &mut AsyncApp| {
                    let result = controller.clear().await;
                    let _ = this.update(&mut *cx, |view, cx| {
                        match result {
                            Ok(snapshot) => view.settings.mark_saved(snapshot),
                            Err(error) => view.settings.mark_failed(error),
                        }
                        cx.notify();
                    });
                },
            ));
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

        fn recover_send(&mut self, transfer_id: drift_core::TransferId, cx: &mut Context<Self>) {
            let controller = Arc::clone(&self.controller);
            self.command_task = Some(cx.spawn(
                async move |this: WeakEntity<MainView>, cx: &mut AsyncApp| {
                    match controller.recover(transfer_id).await {
                        Ok(new_transfer_id) => {
                            let _ = this.update(&mut *cx, |view, cx| {
                                view.send.mark_start_succeeded(new_transfer_id);
                                view.remove_recovery_candidate(transfer_id);
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

        fn discard_send_recovery(
            &mut self,
            transfer_id: drift_core::TransferId,
            cx: &mut Context<Self>,
        ) {
            let controller = Arc::clone(&self.controller);
            self.command_task = Some(cx.spawn(
                async move |this: WeakEntity<MainView>, cx: &mut AsyncApp| {
                    if controller.discard_recovery(transfer_id).await.is_ok() {
                        let _ = this.update(&mut *cx, |view, cx| {
                            view.remove_recovery_candidate(transfer_id);
                            cx.notify();
                        });
                    }
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

        fn recover_receive(
            &mut self,
            transfer_id: drift_core::TransferId,
            code: String,
            destination: std::path::PathBuf,
            cx: &mut Context<Self>,
        ) {
            let controller = Arc::clone(&self.receive_controller);
            self.command_task = Some(cx.spawn(
                async move |this: WeakEntity<MainView>, cx: &mut AsyncApp| {
                    match controller.recover(transfer_id, code, destination).await {
                        Ok(new_transfer_id) => {
                            let _ = this.update(&mut *cx, |view, cx| {
                                view.receive.mark_start_succeeded(new_transfer_id);
                                view.remove_recovery_candidate(transfer_id);
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

        fn discard_receive_recovery(
            &mut self,
            transfer_id: drift_core::TransferId,
            cx: &mut Context<Self>,
        ) {
            let controller = Arc::clone(&self.receive_controller);
            self.command_task = Some(cx.spawn(
                async move |this: WeakEntity<MainView>, cx: &mut AsyncApp| {
                    if controller.discard_recovery(transfer_id).await.is_ok() {
                        let _ = this.update(&mut *cx, |view, cx| {
                            view.remove_recovery_candidate(transfer_id);
                            cx.notify();
                        });
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

        fn retry_receive_transfer(
            &mut self,
            transfer_id: drift_core::TransferId,
            cx: &mut Context<Self>,
        ) {
            let controller = Arc::clone(&self.receive_controller);
            self.command_task = Some(cx.spawn(
                async move |this: WeakEntity<MainView>, cx: &mut AsyncApp| {
                    match controller.retry(transfer_id).await {
                        Ok(new_transfer_id) => {
                            let _ = this.update(&mut *cx, |view, cx| {
                                view.receive.mark_start_succeeded(new_transfer_id);
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

        fn on_settings_key_down(
            &mut self,
            event: &KeyDownEvent,
            _window: &mut Window,
            cx: &mut Context<Self>,
        ) {
            if self.route != MainRoute::Settings
                || event.is_held
                || !self.settings.input_enabled()
            {
                return;
            }
            let keystroke = &event.keystroke;
            if keystroke.key == "backspace" {
                let mut endpoint = self.settings.endpoint().to_owned();
                endpoint.pop();
                self.settings.set_endpoint(endpoint);
                cx.notify();
                return;
            }
            if keystroke.key == "v"
                && (keystroke.modifiers.platform || keystroke.modifiers.control)
            {
                if let Some(text) = cx.read_from_clipboard().and_then(|item| item.text()) {
                    let endpoint = text.replace('\n', "").replace('\r', "");
                    self.settings.set_endpoint(endpoint);
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
                let mut endpoint = self.settings.endpoint().to_owned();
                endpoint.push_str(input);
                self.settings.set_endpoint(endpoint);
                cx.notify();
            }
        }

        fn focus_settings_input(
            &mut self,
            _: &MouseDownEvent,
            window: &mut Window,
            _cx: &mut Context<Self>,
        ) {
            window.focus(&self.settings_focus);
        }

        fn start_choose(&mut self, cx: &mut Context<Self>) {
            let picker = self.path_picker.choose(cx);
            self.command_task = Some(cx.spawn(
                async move |this: WeakEntity<MainView>, cx: &mut AsyncApp| match picker.await {
                    Ok(Some(paths)) if !paths.is_empty() => {
                        let _ = this.update(&mut *cx, |view, cx| {
                            view.start_scan(paths, cx);
                            cx.notify();
                        });
                    }
                    Ok(Some(_)) => {
                        let _ = this.update(&mut *cx, |view, cx| {
                            view.send.cancel_choose();
                            cx.notify();
                        });
                    }
                    Ok(None) => {
                        let _ = this.update(&mut *cx, |view, cx| {
                            view.send.cancel_choose();
                            cx.notify();
                        });
                    }
                    Err(_) => {
                        let _ = this.update(&mut *cx, |view, cx| {
                            view.send.mark_choose_failed();
                            cx.notify();
                        });
                    }
                },
            ));
        }

        fn start_scan(&mut self, paths: Vec<PathBuf>, cx: &mut Context<Self>) {
            self.controller.cancel_scan();
            let Some(generation) = self.send.begin_scan() else {
                return;
            };
            let controller = Arc::clone(&self.controller);
            self.command_task = Some(cx.spawn(
                async move |this: WeakEntity<MainView>, cx: &mut AsyncApp| {
                    let selection = match controller.scan(paths).await {
                        Ok(selection) => selection,
                        Err(error) => {
                            let _ = this.update(&mut *cx, |view, cx| {
                                view.send.mark_scan_failed(generation, error);
                                cx.notify();
                            });
                            return;
                        }
                    };
                    let Some(SendIntent::Preflight { generation, paths }) = this
                        .update(&mut *cx, |view, cx| {
                            let intent = view.send.apply_scan_result(generation, selection);
                            cx.notify();
                            intent
                        })
                        .ok()
                        .flatten()
                    else {
                        return;
                    };
                    if controller.preflight(paths).await.is_ok() {
                        let _ = this.update(&mut *cx, |view, cx| {
                            view.send.mark_preflight_succeeded(generation);
                            cx.notify();
                        });
                    } else {
                        let _ = this.update(&mut *cx, |view, cx| {
                            view.send.mark_preflight_failed(generation);
                            cx.notify();
                        });
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

        fn start_transfer(
            &mut self,
            paths: Vec<std::path::PathBuf>,
            manifest: Option<drift_core::TransferManifest>,
            cx: &mut Context<Self>,
        ) {
            let controller = Arc::clone(&self.controller);
            self.command_task = Some(cx.spawn(
                async move |this: WeakEntity<MainView>, cx: &mut AsyncApp| {
                    match controller.start_send(paths, manifest).await {
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

        fn retry_transfer(&mut self, transfer_id: drift_core::TransferId, cx: &mut Context<Self>) {
            let controller = Arc::clone(&self.controller);
            self.command_task = Some(cx.spawn(
                async move |this: WeakEntity<MainView>, cx: &mut AsyncApp| {
                    match controller.retry(transfer_id).await {
                        Ok(new_transfer_id) => {
                            let _ = this.update(&mut *cx, |view, cx| {
                                view.send.mark_start_succeeded(new_transfer_id);
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

        fn render_error(&self) -> AnyElement {
            div()
                .size_full()
                .p_8()
                .bg(gpui::rgb(0xf7f4ee))
                .text_color(gpui::rgb(0x42251f))
                .child(self.startup_error.clone().unwrap_or_default())
                .into_any_element()
        }

        fn render_home(&mut self, cx: &mut Context<Self>) -> AnyElement {
            let active_count = self
                .transfers
                .rows()
                .iter()
                .filter(|row| {
                    !matches!(
                        row.state,
                        drift_core::TransferState::Completed
                            | drift_core::TransferState::Failed
                            | drift_core::TransferState::Cancelled
                    )
                })
                .count();
            let transfer_count = self.transfers.rows().len();
            let send = cx.listener(|view: &mut MainView, _: &ClickEvent, _, cx| {
                view.route = MainRoute::Send;
                cx.notify();
            });
            let receive = cx.listener(|view: &mut MainView, _: &ClickEvent, _, cx| {
                view.route = MainRoute::Receive;
                cx.notify();
            });
            let transfers = cx.listener(|view: &mut MainView, _: &ClickEvent, _, cx| {
                view.route = MainRoute::Transfers;
                cx.notify();
            });
            div()
                .id("home-view")
                .on_action(cx.listener(|view: &mut MainView, _: &ShowSend, _, cx| {
                    view.route = MainRoute::Send;
                    cx.notify();
                }))
                .on_action(cx.listener(|view: &mut MainView, _: &ShowReceive, _, cx| {
                    view.route = MainRoute::Receive;
                    cx.notify();
                }))
                .on_action(cx.listener(|view: &mut MainView, _: &ShowTransfers, _, cx| {
                    view.route = MainRoute::Transfers;
                    cx.notify();
                }))
                .on_action(cx.listener(|view: &mut MainView, _: &ShowSettings, _, cx| {
                    view.route = MainRoute::Settings;
                    cx.notify();
                }))
                .size_full()
                .p_8()
                .flex()
                .flex_col()
                .gap_4()
                .bg(gpui::rgb(0xf7f4ee))
                .text_color(gpui::rgb(0x1d2a24))
                .child(div().child("Home"))
                .child(self.render_navigation(cx))
                .child(div().child(format!("{active_count} active transfer(s)")))
                .child(div().child(format!("{transfer_count} transfer(s) in this session")))
                .child(
                    div()
                        .flex()
                        .items_center()
                        .gap_2()
                        .child(action_button("home-send", "Send files", true, send))
                        .child(action_button("home-receive", "Receive files", true, receive))
                        .child(action_button(
                            "home-transfers",
                            "View transfers",
                            true,
                            transfers,
                        )),
                )
                .into_any_element()
        }

        fn render_transfers(&mut self, cx: &mut Context<Self>) -> AnyElement {
            let rows = self.transfers.rows().to_vec();
            let selected = self.transfers.selected();
            let mut list = div().flex().flex_col().gap_2();
            for row in rows {
                let transfer_id = row.transfer_id;
                let is_selected = selected == Some(transfer_id);
                let recovery_available = row.recovery_available;
                let recovery_route = row.direction;
                let select = cx.listener(move |view: &mut MainView, _: &ClickEvent, _, cx| {
                    view.transfers.select(Some(transfer_id));
                    if recovery_available {
                        view.route = match recovery_route {
                            super::TransferDirection::Sending => MainRoute::Send,
                            super::TransferDirection::Receiving => MainRoute::Receive,
                        };
                    }
                    cx.notify();
                });
                let progress = if !row.progress_supported {
                    "Progress unavailable".to_owned()
                } else {
                    format_progress(
                        row.progress.transferred_bytes,
                        row.progress.total_bytes,
                        row.speed_bps,
                        row.eta_seconds,
                    )
                };
                let mut row_view = div()
                    .id(SharedString::from(format!("transfer-row-{transfer_id}")))
                    .flex()
                    .flex_col()
                    .gap_1()
                    .p_4()
                    .border_1()
                    .border_color(gpui::rgb(0xd7d0c4))
                    .rounded_sm()
                    .bg(gpui::rgb(0xffffff))
                    .on_click(select)
                    .child(format!(
                        "{} | {} | {}",
                        row.direction.label(),
                        row.display_name,
                        row.file_count_label()
                    ))
                    .child(format!("{} | {}", row.state_label(), progress))
                    .child(row.relay.label());
                if let Some(error) = row.error {
                    row_view = row_view
                        .text_color(gpui::rgb(0x9a3025))
                        .child(error);
                }
                if row.recovery_available {
                    row_view = row_view.child("Recovery available");
                }
                if is_selected {
                    row_view = row_view.border_color(gpui::rgb(0x235347));
                }
                list = list.child(row_view);
            }

            let mut detail = div()
                .flex()
                .flex_col()
                .gap_2()
                .p_4()
                .border_1()
                .border_color(gpui::rgb(0xd7d0c4))
                .rounded_sm()
                .bg(gpui::rgb(0xffffff));
            if let Some(detail_state) = self.transfers.selected_detail() {
                let summary = detail_state.summary;
                let cancel = cx.listener(|view: &mut MainView, _: &ClickEvent, _, cx| {
                    view.dispatch_transfer_action(TransferAction::Cancel, cx);
                });
                let retry = cx.listener(|view: &mut MainView, _: &ClickEvent, _, cx| {
                    view.dispatch_transfer_action(TransferAction::Retry, cx);
                });
                let pause = cx.listener(|view: &mut MainView, _: &ClickEvent, _, cx| {
                    view.dispatch_transfer_action(TransferAction::Pause, cx);
                });
                let resume = cx.listener(|view: &mut MainView, _: &ClickEvent, _, cx| {
                    view.dispatch_transfer_action(TransferAction::Resume, cx);
                });
                let reveal = cx.listener(|view: &mut MainView, _: &ClickEvent, _, cx| {
                    view.dispatch_transfer_action(TransferAction::RevealDestination, cx);
                });
                detail = detail
                    .child("Selected transfer")
                    .child(format!("{}: {}", summary.direction.label(), summary.display_name))
                    .child(summary.state_label())
                    .child(summary.file_count_label())
                    .child(if summary.progress_supported {
                        format_progress(
                            summary.progress.transferred_bytes,
                            summary.progress.total_bytes,
                            summary.speed_bps,
                            summary.eta_seconds,
                        )
                    } else {
                        "Progress unavailable".to_owned()
                    })
                    .child(format!("Relay: {}", summary.relay.label()))
                    .child(action_button(
                        "transfer-cancel",
                        "Cancel",
                        summary.controls.cancel,
                        cancel,
                    ))
                    .child(action_button(
                        "transfer-retry",
                        "Retry",
                        summary.controls.retry,
                        retry,
                    ))
                    .child(action_button(
                        "transfer-pause",
                        "Pause",
                        summary.controls.pause,
                        pause,
                    ))
                    .child(action_button(
                        "transfer-resume",
                        "Resume",
                        summary.controls.resume,
                        resume,
                    ))
                    .child(action_button(
                        "transfer-reveal",
                        "Reveal destination",
                        summary.controls.reveal_destination,
                        reveal,
                    ));
                if let Some(error) = summary.error {
                    detail = detail
                        .text_color(gpui::rgb(0x9a3025))
                        .child(error);
                }
            } else {
                detail = detail.child("Select a transfer to view details.");
            }
            if let Some(error) = &self.transfer_command_error {
                detail = detail
                    .text_color(gpui::rgb(0x9a3025))
                    .child(error.clone());
            }

            let mut root = div()
                .id("transfers-view")
                .key_context("TransfersView")
                .on_action(cx.listener(|view: &mut MainView, _: &ShowHome, _, cx| {
                    view.route = MainRoute::Home;
                    cx.notify();
                }))
                .on_action(cx.listener(|view: &mut MainView, _: &ShowSend, _, cx| {
                    view.route = MainRoute::Send;
                    cx.notify();
                }))
                .on_action(cx.listener(|view: &mut MainView, _: &ShowReceive, _, cx| {
                    view.route = MainRoute::Receive;
                    cx.notify();
                }))
                .on_action(cx.listener(|view: &mut MainView, _: &ShowSettings, _, cx| {
                    view.route = MainRoute::Settings;
                    cx.notify();
                }))
                .size_full()
                .p_8()
                .flex()
                .flex_col()
                .gap_4()
                .bg(gpui::rgb(0xf7f4ee))
                .text_color(gpui::rgb(0x1d2a24))
                .child(div().child("Transfers"))
                .child(self.render_navigation(cx));
            match self.transfers.state() {
                super::TransferListState::Empty => {
                    root = root.child(div().child("No transfers in this session."));
                }
                super::TransferListState::Loading => {
                    root = root.child(div().child("Loading transfers..."));
                }
                super::TransferListState::Error => {
                    root = root.child(div().child("Transfers are unavailable."));
                }
                super::TransferListState::Ready | super::TransferListState::RecoveryAvailable => {
                    root = root.child(list).child(detail);
                }
            }
            root.into_any_element()
        }

        fn render_send(&mut self, cx: &mut Context<Self>) -> AnyElement {
            let phase = self.send.phase();
            let selection = self.send.selection();
            let summary = selection.map(|selection| {
                format!(
                    "{} file(s) / {}",
                    selection.file_count(),
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
                            .child(format_bytes(item.bytes()))
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
            let clear = cx.listener(|view: &mut MainView, _: &ClickEvent, _, cx| {
                view.dispatch_action(SendAction::ClearSelection, cx);
            });
            let mut recovery_panel = div().flex().flex_col().gap_2();
            for candidate in self
                .recovery_candidates
                .iter()
                .filter(|candidate| candidate.kind == RecoveryKind::Send)
                .copied()
            {
                let recover_id = candidate.transfer_id;
                let discard_id = candidate.transfer_id;
                let recover = cx.listener(move |view: &mut MainView, _: &ClickEvent, _, cx| {
                    view.dispatch_action(SendAction::Recover { transfer_id: recover_id }, cx);
                });
                let discard = cx.listener(move |view: &mut MainView, _: &ClickEvent, _, cx| {
                    view.dispatch_action(SendAction::DiscardRecovery { transfer_id: discard_id }, cx);
                });
                recovery_panel = recovery_panel.child(
                    div()
                        .id(SharedString::from(format!("send-recovery-{recover_id}")))
                        .flex()
                        .items_center()
                        .gap_2()
                        .child("Interrupted send available")
                        .child(action_button(
                            SharedString::from(format!("send-recover-{recover_id}")),
                            "Recover",
                            self.send.recovery_enabled(),
                            recover,
                        ))
                        .child(action_button(
                            SharedString::from(format!("send-discard-{discard_id}")),
                            "Discard",
                            self.send.recovery_enabled(),
                            discard,
                        )),
                );
            }

            let mut code_panel = div().flex().items_center().gap_2();
            if let Some(code) = self.send.transfer_code() {
                code_panel = code_panel
                    .child(div().flex_1().child(code.to_owned()))
                    .child(action_button("send-copy-code", "Copy code", true, copy));
            }

            let mut root = div()
                .id("send-view")
                .key_context("SendView")
                .on_action(cx.listener(|view: &mut MainView, _: &ShowHome, _, cx| {
                    view.route = MainRoute::Home;
                    cx.notify();
                }))
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
                .on_action(cx.listener(|view: &mut MainView, _: &RecoverSend, _, cx| {
                    if let Some(candidate) = view
                        .recovery_candidates
                        .iter()
                        .find(|candidate| candidate.kind == RecoveryKind::Send)
                        .copied()
                    {
                        view.dispatch_action(
                            SendAction::Recover {
                                transfer_id: candidate.transfer_id,
                            },
                            cx,
                        );
                    }
                }))
                .on_action(cx.listener(|view: &mut MainView, _: &DiscardSendRecovery, _, cx| {
                    if let Some(candidate) = view
                        .recovery_candidates
                        .iter()
                        .find(|candidate| candidate.kind == RecoveryKind::Send)
                        .copied()
                    {
                        view.dispatch_action(
                            SendAction::DiscardRecovery {
                                transfer_id: candidate.transfer_id,
                            },
                            cx,
                        );
                    }
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
                .on_action(cx.listener(|view: &mut MainView, _: &ShowSettings, _, cx| {
                    view.route = MainRoute::Settings;
                    cx.notify();
                }))
                .on_action(cx.listener(|view: &mut MainView, _: &ShowTransfers, _, cx| {
                    view.route = MainRoute::Transfers;
                    cx.notify();
                }))
                .child(div().child("Send"))
                .child(self.render_navigation(cx))
                .child(div().child(phase.label()))
                .child(recovery_panel)
                .child(
                    div()
                        .id("send-selection")
                        .p_4()
                        .flex()
                        .flex_col()
                        .gap_2()
                        .on_drop(cx.listener(
                            |view: &mut MainView, paths: &ExternalPaths, _, cx| {
                                view.start_scan(paths.paths().to_vec(), cx);
                            },
                        ))
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
                        })
                        .child(action_button(
                            "send-clear-selection",
                            "Clear all",
                            self.send.clear_enabled(),
                            clear,
                        )),
                )
                .child(code_panel)
                .child(
                    div()
                        .flex()
                        .items_center()
                        .gap_2()
                        .child(action_button(
                            "send-choose",
                            "Choose files or folders",
                            self.send.choose_enabled(),
                            choose,
                        ))
                        .child(action_button(
                            "send-start",
                            if self.send.retry_enabled() {
                                "Retry transfer"
                            } else if phase == SendPhase::Failed && self.send.start_enabled() {
                                "Retry check"
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
                    format_progress(
                        progress.transferred,
                        progress.total,
                        self.send.progress_speed_bps(),
                        self.send.progress_eta_seconds(),
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

        fn render_settings(&mut self, cx: &mut Context<Self>) -> AnyElement {
            let save = cx.listener(|view: &mut MainView, _: &ClickEvent, _, cx| {
                view.dispatch_settings_action(SettingsAction::Save, cx);
            });
            let clear = cx.listener(|view: &mut MainView, _: &ClickEvent, _, cx| {
                view.dispatch_settings_action(SettingsAction::Clear, cx);
            });
            let enabled = !self.settings.enabled();
            let toggle = cx.listener(move |view: &mut MainView, _: &ClickEvent, _, cx| {
                view.dispatch_settings_action(SettingsAction::SetEnabled { enabled }, cx);
            });
            let endpoint = if self.settings.endpoint().is_empty() {
                "Enter a Croc-compatible relay endpoint".to_owned()
            } else {
                self.settings.endpoint().to_owned()
            };
            let relay_state = if self.settings.enabled() {
                "Custom relay enabled"
            } else {
                "Default relay behavior"
            };
            let mut root = div()
                .id("settings-view")
                .key_context("SettingsView")
                .on_action(cx.listener(|view: &mut MainView, _: &ShowHome, _, cx| {
                    view.route = MainRoute::Home;
                    cx.notify();
                }))
                .on_action(cx.listener(|view: &mut MainView, _: &ShowSend, _, cx| {
                    view.route = MainRoute::Send;
                    cx.notify();
                }))
                .on_action(cx.listener(|view: &mut MainView, _: &ShowSettings, _, cx| {
                    view.route = MainRoute::Settings;
                    cx.notify();
                }))
                .on_action(cx.listener(|view: &mut MainView, _: &ShowReceive, _, cx| {
                    view.route = MainRoute::Receive;
                    cx.notify();
                }))
                .on_action(cx.listener(|view: &mut MainView, _: &ShowTransfers, _, cx| {
                    view.route = MainRoute::Transfers;
                    cx.notify();
                }))
                .size_full()
                .p_8()
                .flex()
                .flex_col()
                .gap_4()
                .bg(gpui::rgb(0xf7f4ee))
                .text_color(gpui::rgb(0x1d2a24))
                .child(div().child("Settings"))
                .child(self.render_navigation(cx))
                .child(div().child(self.settings.phase().label()))
                .child(div().child(relay_state))
                .child(
                    div()
                        .id("settings-endpoint-input")
                        .track_focus(&self.settings_focus)
                        .on_mouse_down(
                            MouseButton::Left,
                            cx.listener(|view: &mut MainView, event, window, cx| {
                                view.focus_settings_input(event, window, cx);
                            }),
                        )
                        .on_key_down(cx.listener(|view: &mut MainView, event, window, cx| {
                            view.on_settings_key_down(event, window, cx);
                        }))
                        .p_4()
                        .border_1()
                        .border_color(gpui::rgb(0xd7d0c4))
                        .rounded_sm()
                        .bg(gpui::rgb(0xffffff))
                        .child(endpoint),
                )
                .child(
                    div()
                        .flex()
                        .items_center()
                        .gap_2()
                        .child(action_button(
                            "settings-toggle",
                            if self.settings.enabled() {
                                "Disable custom relay"
                            } else {
                                "Enable custom relay"
                            },
                            self.settings.input_enabled(),
                            toggle,
                        ))
                        .child(action_button(
                            "settings-save",
                            "Save relay",
                            self.settings.save_enabled(),
                            save,
                        ))
                        .child(action_button(
                            "settings-clear",
                            "Use default relay",
                            self.settings.clear_enabled(),
                            clear,
                        )),
                );
            if let Some(error) = self.settings.error() {
                root = root.child(div().text_color(gpui::rgb(0x9a3025)).child(error.to_owned()));
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
            let mut recovery_panel = div().flex().flex_col().gap_2();
            for candidate in self
                .recovery_candidates
                .iter()
                .filter(|candidate| candidate.kind == RecoveryKind::Receive)
                .copied()
            {
                let recover_id = candidate.transfer_id;
                let discard_id = candidate.transfer_id;
                let recover = cx.listener(move |view: &mut MainView, _: &ClickEvent, _, cx| {
                    view.dispatch_receive_action(
                        ReceiveAction::Recover {
                            transfer_id: recover_id,
                        },
                        cx,
                    );
                });
                let discard = cx.listener(move |view: &mut MainView, _: &ClickEvent, _, cx| {
                    view.dispatch_receive_action(
                        ReceiveAction::DiscardRecovery {
                            transfer_id: discard_id,
                        },
                        cx,
                    );
                });
                recovery_panel = recovery_panel.child(
                    div()
                        .id(SharedString::from(format!("receive-recovery-{recover_id}")))
                        .flex()
                        .items_center()
                        .gap_2()
                        .child("Interrupted receive available")
                        .child(action_button(
                            SharedString::from(format!("receive-recover-{recover_id}")),
                            "Recover",
                            self.receive.recovery_enabled(),
                            recover,
                        ))
                        .child(action_button(
                            SharedString::from(format!("receive-discard-{discard_id}")),
                            "Discard",
                            self.receive.discard_recovery_enabled(),
                            discard,
                        )),
                );
            }
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
                .on_action(cx.listener(|view: &mut MainView, _: &ShowHome, _, cx| {
                    view.route = MainRoute::Home;
                    cx.notify();
                }))
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
                .on_action(cx.listener(|view: &mut MainView, _: &RecoverReceive, _, cx| {
                    if let Some(candidate) = view
                        .recovery_candidates
                        .iter()
                        .find(|candidate| candidate.kind == RecoveryKind::Receive)
                        .copied()
                    {
                        view.dispatch_receive_action(
                            ReceiveAction::Recover {
                                transfer_id: candidate.transfer_id,
                            },
                            cx,
                        );
                    }
                }))
                .on_action(cx.listener(|view: &mut MainView, _: &DiscardReceiveRecovery, _, cx| {
                    if let Some(candidate) = view
                        .recovery_candidates
                        .iter()
                        .find(|candidate| candidate.kind == RecoveryKind::Receive)
                        .copied()
                    {
                        view.dispatch_receive_action(
                            ReceiveAction::DiscardRecovery {
                                transfer_id: candidate.transfer_id,
                            },
                            cx,
                        );
                    }
                }))
                .on_action(cx.listener(|view: &mut MainView, _: &ShowTransfers, _, cx| {
                    view.route = MainRoute::Transfers;
                    cx.notify();
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
                .child(recovery_panel)
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
                            if phase == ReceivePhase::Failed && self.receive.preflight_enabled() {
                                "Retry check"
                            } else {
                                "Check Croc"
                            },
                            self.receive.preflight_enabled(),
                            preflight,
                        ))
                        .child(action_button(
                            "receive-start",
                            if self.receive.retry_enabled() {
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
                    format_progress(
                        progress.0,
                        progress.1,
                        self.receive.progress_speed_bps(),
                        self.receive.progress_eta_seconds(),
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
            let home = cx.listener(|view: &mut MainView, _: &ClickEvent, _, cx| {
                view.route = MainRoute::Home;
                cx.notify();
            });
            let send = cx.listener(|view: &mut MainView, _: &ClickEvent, _, cx| {
                view.route = MainRoute::Send;
                cx.notify();
            });
            let receive = cx.listener(|view: &mut MainView, _: &ClickEvent, _, cx| {
                view.route = MainRoute::Receive;
                cx.notify();
            });
            let transfers = cx.listener(|view: &mut MainView, _: &ClickEvent, _, cx| {
                view.route = MainRoute::Transfers;
                cx.notify();
            });
            let settings = cx.listener(|view: &mut MainView, _: &ClickEvent, _, cx| {
                view.route = MainRoute::Settings;
                cx.notify();
            });
            div()
                .flex()
                .items_center()
                .gap_2()
                .child(action_button("route-home", "Home", true, home))
                .child(action_button("route-send", "Send", true, send))
                .child(action_button(
                    "route-receive",
                    "Receive",
                    true,
                    receive,
                ))
                .child(action_button(
                    "route-transfers",
                    "Transfers",
                    true,
                    transfers,
                ))
                .child(action_button("route-settings", "Settings", true, settings))
                .into_any_element()
        }
    }

    impl Render for MainView {
        fn render(&mut self, _window: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
            if self.startup_error.is_some() {
                self.render_error()
            } else {
                match self.route {
                    MainRoute::Home => self.render_home(cx),
                    MainRoute::Send => self.render_send(cx),
                    MainRoute::Receive => self.render_receive(cx),
                    MainRoute::Transfers => self.render_transfers(cx),
                    MainRoute::Settings => self.render_settings(cx),
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

    fn format_progress(
        transferred: u64,
        total: u64,
        speed_bps: Option<u64>,
        eta_seconds: Option<u64>,
    ) -> String {
        let total = if total == 0 {
            "total unavailable".to_owned()
        } else {
            format_bytes(total)
        };
        let speed = speed_bps.map_or_else(
            || "speed unavailable".to_owned(),
            |speed_bps| format!("{}/s", format_bytes(speed_bps)),
        );
        let eta = eta_seconds.map_or_else(
            || "ETA unavailable".to_owned(),
            format_eta,
        );
        format!("{} / {} | {} | ETA {eta}", format_bytes(transferred), total, speed)
    }

    fn format_eta(seconds: u64) -> String {
        const MINUTE: u64 = 60;
        const HOUR: u64 = MINUTE * 60;
        const DAY: u64 = HOUR * 24;
        if seconds >= DAY {
            format!("{}d {}h", seconds / DAY, (seconds % DAY) / HOUR)
        } else if seconds >= HOUR {
            format!("{}h {}m", seconds / HOUR, (seconds % HOUR) / MINUTE)
        } else if seconds >= MINUTE {
            format!("{}m {}s", seconds / MINUTE, seconds % MINUTE)
        } else {
            format!("{seconds}s")
        }
    }

    pub fn run_with_controller(controller: Arc<dyn SendController>) {
        run_with_controllers(controller, Arc::new(UnavailableReceiveController));
    }

    pub fn run_with_controllers(
        controller: Arc<dyn SendController>,
        receive_controller: Arc<dyn ReceiveController>,
    ) {
        run_with_controllers_and_settings(
            controller,
            receive_controller,
            Arc::new(UnavailableSettingsController),
        );
    }

    pub fn run_with_controllers_and_settings(
        controller: Arc<dyn SendController>,
        receive_controller: Arc<dyn ReceiveController>,
        settings_controller: Arc<dyn SettingsController>,
    ) {
        run_with_controllers_and_settings_and_transfers(
            controller,
            receive_controller,
            settings_controller,
            Arc::new(UnavailableTransferController),
        );
    }

    pub fn run_with_controllers_and_settings_and_transfers(
        controller: Arc<dyn SendController>,
        receive_controller: Arc<dyn ReceiveController>,
        settings_controller: Arc<dyn SettingsController>,
        transfer_controller: Arc<dyn TransferController>,
    ) {
        run_with_startup_error_and_controllers(
            None,
            controller,
            receive_controller,
            settings_controller,
            transfer_controller,
        );
    }

    pub fn run_with_startup_error(startup_error: Option<String>) {
        run_with_startup_error_and_controllers(
            startup_error,
            Arc::new(UnavailableSendController),
            Arc::new(UnavailableReceiveController),
            Arc::new(UnavailableSettingsController),
            Arc::new(UnavailableTransferController),
        );
    }

    fn run_with_startup_error_and_controllers(
        startup_error: Option<String>,
        controller: Arc<dyn SendController>,
        receive_controller: Arc<dyn ReceiveController>,
        settings_controller: Arc<dyn SettingsController>,
        transfer_controller: Arc<dyn TransferController>,
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
                        settings_controller,
                        transfer_controller,
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
    run, run_with_controller, run_with_controllers, run_with_controllers_and_settings,
    run_with_controllers_and_settings_and_transfers, run_with_startup_error, MainView,
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
pub fn run_with_controllers_and_settings(
    _controller: std::sync::Arc<dyn SendController>,
    _receive_controller: std::sync::Arc<dyn ReceiveController>,
    _settings_controller: std::sync::Arc<dyn SettingsController>,
) {
    eprintln!("drift GUI disabled; rebuild with --features gui");
}

#[cfg(not(feature = "gui"))]
pub fn run_with_controllers_and_settings_and_transfers(
    _controller: std::sync::Arc<dyn SendController>,
    _receive_controller: std::sync::Arc<dyn ReceiveController>,
    _settings_controller: std::sync::Arc<dyn SettingsController>,
    _transfer_controller: std::sync::Arc<dyn TransferController>,
) {
    eprintln!("drift GUI disabled; rebuild with --features gui");
}

#[cfg(not(feature = "gui"))]
pub fn run() {
    run_with_startup_error(None);
}
