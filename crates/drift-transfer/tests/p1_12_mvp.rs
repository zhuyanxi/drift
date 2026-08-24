#![cfg(unix)]

mod support;

use drift_core::{
    ResumeRequest, ResumeState, TransferError, TransferEvent, TransferFailureKind, TransferId,
    TransferState,
};
use drift_protocol::{ReceiveRequest, SendRequest};
use drift_storage::{scan_send_paths, JsonStore, ScanCancellation};
use drift_transfer::{TransferManager, TransferNotification};
use std::{
    fs,
    path::{Path, PathBuf},
    time::Duration,
};
use support::{FakeCrocBehavior, Harness, TEST_CODE};
use tokio::sync::broadcast;

const TEST_TIMEOUT: Duration = Duration::from_secs(5);

#[tokio::test]
async fn single_file_transfer_preserves_bytes_across_fake_croc_pair() {
    let harness = Harness::new();
    let source = harness.source("single.txt", b"single-file payload\n");
    let (destination, sender, receiver) = run_pair(&harness, vec![source]).await;

    assert_eq!(
        fs::read(destination.join("single.txt")).unwrap(),
        b"single-file payload\n"
    );
    assert_eq!(sender.state, TransferState::Completed);
    assert_eq!(sender.code.as_deref(), Some(TEST_CODE));
    assert_eq!(receiver.state, TransferState::Completed);
    assert_no_staging(&destination);
}

#[tokio::test]
async fn multi_file_directory_transfer_preserves_relative_layout_and_bytes() {
    let harness = Harness::new();
    let first = harness.source("alpha.txt", b"alpha");
    let nested = harness.source("bundle/nested.txt", b"nested");
    let leaf = harness.source("bundle/deep/leaf.bin", &[0, 1, 2, 3, 255]);
    let bundle = harness.path("bundle");

    let (destination, sender, receiver) = run_pair(&harness, vec![first, bundle]).await;

    assert_eq!(sender.manifest.as_ref().unwrap().files.len(), 3);
    assert_eq!(fs::read(destination.join("alpha.txt")).unwrap(), b"alpha");
    assert_eq!(
        fs::read(destination.join("bundle/nested.txt")).unwrap(),
        b"nested"
    );
    assert_eq!(
        fs::read(destination.join("bundle/deep/leaf.bin")).unwrap(),
        &[0, 1, 2, 3, 255]
    );
    assert_eq!(receiver.state, TransferState::Completed);
    assert_no_staging(&destination);
    assert!(nested.exists());
    assert!(leaf.exists());
}

#[tokio::test]
async fn process_failure_is_typed_safe_and_not_retryable() {
    let harness = Harness::new();
    let source = harness.source("failure.txt", b"failure");
    let manager =
        TransferManager::new(harness.backend(FakeCrocBehavior::ProcessFailure, TEST_TIMEOUT));
    let mut events = manager.subscribe();
    let transfer_id = manager
        .start_send(SendRequest::new(vec![source]).unwrap())
        .await
        .unwrap();

    assert_eq!(
        wait_for_terminal(&mut events, transfer_id).await,
        TransferEvent::Failed
    );
    let session = manager.session(transfer_id).await.unwrap();
    assert_eq!(session.state, TransferState::Failed);
    assert_eq!(
        session.failure_kind,
        Some(TransferFailureKind::ProcessFailure)
    );
    assert_eq!(session.error.as_deref(), Some("backend operation failed"));
    assert!(!session
        .error
        .as_deref()
        .unwrap()
        .contains("private integration"));
    assert_eq!(
        manager.retry(transfer_id).await,
        Err(TransferError::RetryNotAllowed(TransferState::Failed))
    );
}

#[tokio::test]
async fn relay_failure_is_typed_safe_and_not_retryable() {
    let harness = Harness::new();
    let source = harness.source("relay-failure.txt", b"failure");
    let manager = TransferManager::new(
        harness
            .backend(FakeCrocBehavior::RelayFailure, Duration::from_millis(50))
            .with_relay("relay.invalid"),
    );
    let mut events = manager.subscribe();
    let transfer_id = manager
        .start_send(SendRequest::new(vec![source]).unwrap())
        .await
        .unwrap();

    assert_eq!(
        wait_for_terminal(&mut events, transfer_id).await,
        TransferEvent::Failed
    );
    let session = manager.session(transfer_id).await.unwrap();
    assert_eq!(session.state, TransferState::Failed);
    assert_eq!(session.failure_kind, Some(TransferFailureKind::Network));
    assert_eq!(
        session.error.as_deref(),
        Some("backend operation timed out")
    );
    let retry_id = manager.retry(transfer_id).await.unwrap();
    assert_ne!(retry_id, transfer_id);
    manager.cancel(retry_id).await.unwrap();
    assert_eq!(
        manager.session(retry_id).await.unwrap().state,
        TransferState::Cancelled
    );
}

#[tokio::test]
async fn failed_receive_removes_unverified_output_and_staging() {
    let harness = Harness::new();
    let source = harness.source("source.txt", b"source");
    let destination = harness.destination();
    let sender = TransferManager::new(harness.backend(FakeCrocBehavior::Pair, TEST_TIMEOUT));
    let receiver = TransferManager::new(
        harness.backend(FakeCrocBehavior::ReceivePartialFailure, TEST_TIMEOUT),
    );
    let mut sender_events = sender.subscribe();
    let mut receiver_events = receiver.subscribe();
    let receiver_id = receiver
        .start_receive(ReceiveRequest::new(TEST_CODE, &destination).unwrap())
        .await
        .unwrap();
    let scan = scan_send_paths(vec![source.clone()], ScanCancellation::new())
        .await
        .unwrap();
    let sender_id = sender
        .start_send_with_manifest(
            SendRequest::new(vec![source]).unwrap(),
            Some(scan.manifest().clone()),
        )
        .await
        .unwrap();

    assert_eq!(
        wait_for_terminal(&mut sender_events, sender_id).await,
        TransferEvent::Completed
    );
    assert_eq!(
        wait_for_terminal(&mut receiver_events, receiver_id).await,
        TransferEvent::Failed
    );
    let session = receiver.session(receiver_id).await.unwrap();
    assert_eq!(
        session.failure_kind,
        Some(TransferFailureKind::ProcessFailure)
    );
    assert!(!destination.join("file.txt").exists());
    wait_for_no_staging(&destination).await;
    assert_no_staging(&destination);
}

#[tokio::test]
async fn cancellation_reaps_transfer_and_croc_controls_report_unavailable() {
    let harness = Harness::new();
    let source = harness.source("cancel.txt", b"cancel");
    let manager = TransferManager::new(harness.backend(FakeCrocBehavior::Slow, TEST_TIMEOUT));
    let mut events = manager.subscribe();
    let transfer_id = manager
        .start_send(SendRequest::new(vec![source]).unwrap())
        .await
        .unwrap();

    assert_eq!(
        manager.pause(transfer_id).await,
        Err(TransferError::CapabilityUnavailable(
            drift_core::TransferCapability::Pause
        ))
    );
    assert_eq!(
        manager.resume(transfer_id).await,
        Err(TransferError::CapabilityUnavailable(
            drift_core::TransferCapability::Resume
        ))
    );
    manager.cancel(transfer_id).await.unwrap();
    assert_eq!(
        wait_for_terminal(&mut events, transfer_id).await,
        TransferEvent::Cancelled
    );
    assert_eq!(
        manager.session(transfer_id).await.unwrap().state,
        TransferState::Cancelled
    );
}

#[tokio::test]
async fn retryable_timeout_persists_recovery_and_accepts_cancel_restart() {
    let harness = Harness::new();
    let source = harness.source("recover.txt", b"recover");
    let scan = scan_send_paths(vec![source.clone()], ScanCancellation::new())
        .await
        .unwrap();
    let store = harness.resume_store();
    let manager = TransferManager::with_resume_store(
        harness.backend(FakeCrocBehavior::Slow, Duration::from_millis(50)),
        "croc",
        store.clone(),
    );
    let mut events = manager.subscribe();
    let transfer_id = manager
        .start_send_with_manifest(
            SendRequest::new(vec![source]).unwrap(),
            Some(scan.manifest().clone()),
        )
        .await
        .unwrap();

    assert_eq!(
        wait_for_terminal(&mut events, transfer_id).await,
        TransferEvent::Failed
    );
    let resume = wait_for_resume(&store, transfer_id).await;
    assert!(!resume.capabilities.resume);
    assert!(matches!(resume.request, ResumeRequest::Send { .. }));
    let replacement_id = manager.recover(resume, None, None).await.unwrap();
    assert_ne!(replacement_id, transfer_id);
    assert_eq!(
        wait_for_terminal(&mut events, replacement_id).await,
        TransferEvent::Failed
    );
    assert_eq!(store.load_resume(transfer_id).await.unwrap(), None);
}

#[tokio::test]
async fn receive_rejects_file_destination_before_backend_start() {
    let harness = Harness::new();
    let destination = harness.path("not-a-directory");
    fs::write(&destination, b"existing").unwrap();
    let manager =
        TransferManager::new(harness.backend(FakeCrocBehavior::ProcessFailure, TEST_TIMEOUT));

    let result = manager
        .start_receive(ReceiveRequest::new(TEST_CODE, &destination).unwrap())
        .await;
    assert!(matches!(
        result,
        Err(TransferError::Filesystem(message)) if message == "receive destination is unavailable"
    ));
}

async fn run_pair(
    harness: &Harness,
    source_paths: Vec<PathBuf>,
) -> (
    PathBuf,
    drift_core::TransferSession,
    drift_core::TransferSession,
) {
    let scan = scan_send_paths(source_paths.clone(), ScanCancellation::new())
        .await
        .unwrap();
    let backend = harness.backend(FakeCrocBehavior::Pair, TEST_TIMEOUT);
    let sender = TransferManager::new(backend.clone());
    let receiver = TransferManager::new(backend);
    let destination = harness.destination();
    let mut receiver_events = receiver.subscribe();
    let mut sender_events = sender.subscribe();
    let receiver_id = receiver
        .start_receive(ReceiveRequest::new(TEST_CODE, &destination).unwrap())
        .await
        .unwrap();
    let sender_id = sender
        .start_send_with_manifest(
            SendRequest::new(source_paths).unwrap(),
            Some(scan.manifest().clone()),
        )
        .await
        .unwrap();

    assert_eq!(
        wait_for_terminal(&mut sender_events, sender_id).await,
        TransferEvent::Completed
    );
    assert_eq!(
        wait_for_terminal(&mut receiver_events, receiver_id).await,
        TransferEvent::Completed
    );
    (
        destination,
        sender.session(sender_id).await.unwrap(),
        receiver.session(receiver_id).await.unwrap(),
    )
}

async fn wait_for_terminal(
    events: &mut broadcast::Receiver<TransferNotification>,
    transfer_id: TransferId,
) -> TransferEvent {
    loop {
        let notification = tokio::time::timeout(TEST_TIMEOUT, events.recv())
            .await
            .unwrap()
            .unwrap();
        if notification.transfer_id == transfer_id
            && matches!(
                notification.event,
                TransferEvent::Completed | TransferEvent::Failed | TransferEvent::Cancelled
            )
        {
            return notification.event;
        }
    }
}

async fn wait_for_resume(store: &JsonStore, transfer_id: TransferId) -> ResumeState {
    tokio::time::timeout(TEST_TIMEOUT, async {
        loop {
            if let Some(state) = store.load_resume(transfer_id).await.unwrap() {
                return state;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap()
}

fn assert_no_staging(destination: &Path) {
    assert!(!fs::read_dir(destination).unwrap().any(|entry| entry
        .unwrap()
        .file_name()
        .to_string_lossy()
        .starts_with(".drift-staging-")));
}

async fn wait_for_no_staging(destination: &Path) {
    tokio::time::timeout(TEST_TIMEOUT, async {
        loop {
            let has_staging = fs::read_dir(destination).unwrap().any(|entry| {
                entry
                    .unwrap()
                    .file_name()
                    .to_string_lossy()
                    .starts_with(".drift-staging-")
            });
            if !has_staging {
                return;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap();
}
