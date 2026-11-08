//! Handler-test harness: a real `AppState` over an UNINITIALISED single-node
//! Raft (so it is never leader: leader-gated handlers must refuse, and
//! `client_write` fails) plus an in-memory state machine the test can seed.
#![allow(unused_imports)]

use super::*;

pub(super) struct Harness {
    pub state: Arc<AppState>,
    _dir: tempfile::TempDir,
}

pub(super) async fn harness() -> Harness {
    let dir = tempfile::tempdir().unwrap();
    let mut config: ManagerConfig = toml::from_str(include_str!("../../../../deploy/examples/manager.toml"))
        .expect("example manager.toml parses");
    config.cluster.shared_secret = String::new();
    config.audit.replicate_via_raft = false; // read the local ring
    config.node.data_dir = dir.path().to_path_buf();

    let log_store = dtfs_raft::log_store::LogStore::new_with_opts(
        dir.path().join("raft-log"),
        dtfs_raft::log_store::RocksDbLogOpts::default(),
    )
    .unwrap();
    let sm = dtfs_raft::state_machine::StateMachineStore::open_at(&dir.path().join("raft-sm")).unwrap();
    let sm_data = sm.data().clone();
    let tls = dtfs_common::tls::TlsContext::from_config(&config.tls).unwrap();
    let raft_cfg = Arc::new(openraft::Config::default().validate().unwrap());
    let raft = Arc::new(
        openraft::Raft::new(
            1,
            raft_cfg,
            dtfs_raft::network::NetworkFactory { tls: tls.clone(), shared_secret: String::new(), self_id: 1 },
            log_store,
            sm,
        )
        .await
        .unwrap(),
    );
    let audit_log = Arc::new(AuditLog::new(1000));
    let join = Arc::new(JoinHandler::new(
        String::new(),
        config.cluster.name.clone(),
        raft.clone(),
        audit_log.clone(),
    ));
    let scheduler = Arc::new(TaskScheduler::new(
        raft.clone(),
        sm_data.clone(),
        audit_log.clone(),
        60,
        60,
        tls,
        config.placement.clone(),
    ));
    let state = Arc::new(AppState {
        config,
        raft,
        sm_data,
        audit_log,
        join,
        scheduler,
        io_rate: Arc::new(parking_lot::RwLock::new(IoRateTracker::default())),
        memory: None,
    });
    Harness { state, _dir: dir }
}
