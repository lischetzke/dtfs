//! Unit-test modules lifted out of `state_machine.rs` so the source file
//! stays readable; every module below sees the parent's private items
//! through the glob import.
#![allow(unused_imports, deprecated)]

use super::*;

/// Hold a lease with id "seed" so a test can seed chunk rows through a
/// leased UpdateChunkLocations (lease-less writes cannot add indexes).
fn seed_lease(sm: &mut StateMachineData, path: &str) {
    use openraft::vote::RaftLeaderId;
    let leader = openraft::impls::leader_id_adv::LeaderId::<u64, u64>::new(0, 0);
    let log_id: LogId = openraft::LogId::new(leader, 9_000);
    apply_request(sm, &log_id, &ClientRequest::AcquireWriteLease {
        path: path.into(), owner_id: "seed".into(), lease_ms: 60_000, now_ms: 1,
        proposed_lease_id: "seed".into(),
    });
}

#[cfg(test)]
mod snapshot_persistence_tests {
    use super::*;
    use openraft::storage::{RaftSnapshotBuilder, RaftStateMachine};

    fn tmp_dir() -> tempfile::TempDir {
        tempfile::tempdir().expect("create tempdir")
    }

    /// Build a tiny FSM populated with some files + nodes and a synthetic
    /// `last_applied_log` so we can detect that a restart restores it.
    fn populate_sample_state(data: &mut StateMachineData) {
        use openraft::vote::RaftLeaderId;
        let leader_id =
            openraft::impls::leader_id_adv::LeaderId::<u64, u64>::new(0, 0);
        let log_id = openraft::LogId::new(leader_id, 100);
        data.last_applied_log = Some(log_id);
        data.files.insert(
            "/a.bin".into(),
            FileMetadata {
                path: "/a.bin".into(),
                volume_id: "default".into(),
                chunk_size: 1024,
                chunks: vec![],
                created_at_ms: 1,
                updated_at_ms: 1,
                symlink_target: None,
                logical_size: None,
                is_directory: false,
                cache_cooldown_until_ms: std::collections::BTreeMap::new(), ever_committed: None,
            },
        );
        data.cluster_config.version = 7;
        data.cluster_config.config_json = "{\"v\":7}".into();
    }

    #[tokio::test]
    async fn build_snapshot_persists_and_reopen_restores_state() {
        let dir = tmp_dir();
        let path = dir.path().to_path_buf();

        // Build a state machine, populate it, and produce a snapshot.
        {
            let mut sm = StateMachineStore::open_at(&path).expect("open");
            {
                let mut data = sm.data.write().await;
                populate_sample_state(&mut data);
            }
            let mut builder = sm.get_snapshot_builder().await;
            let snap = builder.build_snapshot().await.expect("build");
            assert!(snap.meta.last_log_id.is_some());
        }

        // Disk file should now exist.
        let on_disk = path.join("snapshot.bin");
        assert!(
            on_disk.exists(),
            "snapshot.bin must be written to disk so restart can restore FSM"
        );

        // Reopen with a fresh store at the same path — should recover state.
        let restored = StateMachineStore::open_at(&path).expect("reopen");
        let data = restored.data.read().await;
        assert_eq!(
            data.files.len(),
            1,
            "files BTreeMap restored from snapshot"
        );
        assert!(
            data.files.contains_key("/a.bin"),
            "the specific file we created is back"
        );
        assert_eq!(data.cluster_config.version, 7, "cluster config restored");
        assert!(
            data.last_applied_log.is_some(),
            "last_applied_log restored — without this Raft skips log replay and stays empty"
        );
    }

    #[tokio::test]
    async fn open_at_empty_dir_is_blank_state_machine() {
        let dir = tmp_dir();
        let sm = StateMachineStore::open_at(dir.path()).expect("open empty");
        let data = sm.data.read().await;
        assert!(data.files.is_empty());
        assert!(data.last_applied_log.is_none());
    }

    /// Bug BP-2 — a truncated/0-byte `snapshot.bin` (disk-full mid-write)
    /// must NOT hard-error `open_at`; it is treated as absent, removed, and
    /// the store starts blank so openraft catches up via log replay.
    #[tokio::test]
    async fn bp_2_truncated_snapshot_treated_as_absent() {
        let dir = tmp_dir();
        // Write a 0-byte snapshot.bin (the exact F3 corruption shape).
        std::fs::write(dir.path().join("snapshot.bin"), b"").expect("write empty");
        let sm = StateMachineStore::open_at(dir.path())
            .expect("open must NOT error on a truncated snapshot");
        {
            let data = sm.data.read().await;
            assert!(data.files.is_empty(), "blank state after discarding corrupt snapshot");
        }
        // The corrupt file is removed so it cannot wedge subsequent opens.
        assert!(
            !dir.path().join("snapshot.bin").exists(),
            "corrupt snapshot.bin must be deleted"
        );
        // A partial (non-bincode) payload is likewise tolerated.
        std::fs::write(dir.path().join("snapshot.bin"), b"\x00\x01garbage").expect("write junk");
        assert!(
            StateMachineStore::open_at(dir.path()).is_ok(),
            "partial snapshot envelope tolerated"
        );
    }

    /// Bug BP-2 — a stray `snapshot.bin.tmp` left by an interrupted write is
    /// ignored on open and does not become the active snapshot.
    #[tokio::test]
    async fn bp_2_stray_tmp_ignored_on_open() {
        let dir = tmp_dir();
        std::fs::write(dir.path().join("snapshot.bin.tmp"), b"partial").expect("write tmp");
        let sm = StateMachineStore::open_at(dir.path()).expect("open ignores .tmp");
        {
            let data = sm.data.read().await;
            assert!(data.files.is_empty());
        }
        assert!(
            !dir.path().join("snapshot.bin.tmp").exists(),
            "stray .tmp is cleaned up on open"
        );
    }

    #[tokio::test]
    async fn install_snapshot_persists_to_disk() {
        let dir = tmp_dir();

        // Source SM produces a snapshot.
        let snapshot_bytes;
        let snapshot_meta;
        {
            let mut src = StateMachineStore::open_at(dir.path()).expect("open src");
            {
                let mut data = src.data.write().await;
                populate_sample_state(&mut data);
            }
            let mut builder = src.get_snapshot_builder().await;
            let snap = builder.build_snapshot().await.expect("build");
            snapshot_meta = snap.meta.clone();
            snapshot_bytes = snap.snapshot.into_inner();
        }

        // A fresh SM in a different dir simulates a follower receiving the snapshot.
        let follower_dir = tmp_dir();
        let mut follower = StateMachineStore::open_at(follower_dir.path()).expect("open follower");
        follower
            .install_snapshot(&snapshot_meta, Cursor::new(snapshot_bytes))
            .await
            .expect("install");

        // The follower must now have the file *and* must have written
        // snapshot.bin so its own restart works.
        let path = follower_dir.path().join("snapshot.bin");
        assert!(path.exists(), "install_snapshot must persist to disk");

        // Closing and reopening the follower restores the state.
        drop(follower);
        let reopened = StateMachineStore::open_at(follower_dir.path()).expect("reopen follower");
        let data = reopened.data.read().await;
        assert_eq!(data.files.len(), 1);
        assert_eq!(data.cluster_config.version, 7);
    }

    /// Audit: a failed snapshot persist must surface as Err and leave the
    /// in-memory current snapshot untouched (openraft purges the log after Ok).
    #[tokio::test]
    async fn build_snapshot_persist_failure_is_err_and_not_published() {
        let dir = tmp_dir();
        let path = dir.path().join("sm");
        let mut sm = StateMachineStore::open_at(&path).expect("open");
        {
            let mut data = sm.data.write().await;
            populate_sample_state(&mut data);
        }
        std::fs::remove_dir_all(&path).expect("make snapshot dir unwritable");
        let mut builder = sm.get_snapshot_builder().await;
        assert!(builder.build_snapshot().await.is_err());
        assert!(sm.get_current_snapshot().await.unwrap().is_none());

        // Negative control: writable dir succeeds and publishes.
        std::fs::create_dir_all(&path).unwrap();
        let mut builder = sm.get_snapshot_builder().await;
        builder.build_snapshot().await.expect("build ok");
        assert!(sm.get_current_snapshot().await.unwrap().is_some());
    }

    #[tokio::test]
    async fn install_snapshot_persist_failure_is_err_and_state_unchanged() {
        let src_dir = tmp_dir();
        let mut src = StateMachineStore::open_at(src_dir.path()).expect("open src");
        {
            let mut data = src.data.write().await;
            populate_sample_state(&mut data);
        }
        let snap = src.get_snapshot_builder().await.build_snapshot().await.expect("build");
        let meta = snap.meta.clone();
        let bytes = snap.snapshot.into_inner();

        let dir = tmp_dir();
        let path = dir.path().join("sm");
        let mut f = StateMachineStore::open_at(&path).expect("open follower");
        std::fs::remove_dir_all(&path).unwrap();
        assert!(f.install_snapshot(&meta, Cursor::new(bytes)).await.is_err());
        assert!(f.data.read().await.files.is_empty());
        assert!(f.get_current_snapshot().await.unwrap().is_none());
    }

    /// Bug AZ-3 — `FileMetadata.chunk_size` is per-file and snapshotted at
    /// `CreateFile` time. An operator UpsertVolume call that changes
    /// `VolumeConfig.chunk_size_bytes` MUST NOT retro-modify any existing
    /// `FileMetadata.chunk_size`. The read path resolves chunk indices
    /// against the per-file value
    /// (`dtfs-fuse/src/filesystem.rs::resolve_chunk_size`), so leaving
    /// FileMetadata alone is the load-bearing invariant — without it,
    /// files written before the resize become unreadable because
    /// `offset / chunk_size` produces an out-of-range chunk_index.
    #[tokio::test]
    async fn bug_az_3_volume_chunk_size_change_does_not_touch_existing_file() {
        use dtfs_common::types::{
            CacheReplication, TierReplication, VolumeConfig, VolumeId, WriteSafety,
        };
        use openraft::vote::RaftLeaderId;

        fn mk_log_id(idx: u64) -> LogId {
            let leader = openraft::impls::leader_id_adv::LeaderId::<u64, u64>::new(0, 0);
            openraft::LogId::new(leader, idx)
        }

        let dir = tempfile::TempDir::new().expect("tmpdir");
        let store = StateMachineStore::open_at(dir.path()).expect("open sm");
        let mut sm = store.data.write().await;

        let volume = VolumeConfig {
            id: VolumeId::new("default".to_string()),
            name: "default".to_string(),
            chunk_size_bytes: 16 * 1024 * 1024,
            replication: TierReplication { hot: 0, warm: 0, cold: 1, tape: 0 },
            mandatory_replicas: TierReplication { hot: 0, warm: 0, cold: 1, tape: 0 },
            cache_replicas: CacheReplication::default(),
            anchor: None,
            spill: dtfs_common::types::SpillPolicy::default(),
            version: 0,
            anchor_datacenter: None,
            write_safety: WriteSafety {
                min_replicas_before_ack: 1,
                write_timeout_secs: 30,
            },
        };
        apply_request(
            &mut sm,
            &mk_log_id(1),
            &ClientRequest::UpsertVolume { now_ms: 0, config: volume.clone(), expected_version: None },
        );
        apply_request(
            &mut sm,
            &mk_log_id(2),
            &ClientRequest::CreateFile { now_ms: 0,
                path: "/foo.bin".to_string(),
                volume_id: "default".to_string(),
                chunk_size: 16 * 1024 * 1024,
            },
        );

        let mut shrunk = volume.clone();
        shrunk.chunk_size_bytes = 4 * 1024 * 1024;
        apply_request(
            &mut sm,
            &mk_log_id(3),
            &ClientRequest::UpsertVolume { now_ms: 0, config: shrunk, expected_version: None },
        );

        let file = sm.files.get("/foo.bin").expect("file persisted");
        assert_eq!(
            file.chunk_size,
            16 * 1024 * 1024,
            "FileMetadata.chunk_size must NOT track volume.chunk_size_bytes; \
             the read path uses the per-file value to compute chunk indices",
        );
        let vol = sm.volumes.get("default").expect("volume present");
        assert_eq!(vol.chunk_size_bytes, 4 * 1024 * 1024);
    }

    /// Bug AZ-3 — the inverse: a NEW file created after the resize picks up
    /// the NEW volume.chunk_size_bytes. Pins the contract that the
    /// CreateFile caller (FUSE / dtfs-cli) reads volume.chunk_size_bytes
    /// at write time and snapshots it onto the file.
    #[tokio::test]
    async fn bug_az_3_new_file_after_resize_uses_new_chunk_size() {
        use dtfs_common::types::{
            CacheReplication, TierReplication, VolumeConfig, VolumeId, WriteSafety,
        };
        use openraft::vote::RaftLeaderId;

        fn mk_log_id(idx: u64) -> LogId {
            let leader = openraft::impls::leader_id_adv::LeaderId::<u64, u64>::new(0, 0);
            openraft::LogId::new(leader, idx)
        }

        let dir = tempfile::TempDir::new().expect("tmpdir");
        let store = StateMachineStore::open_at(dir.path()).expect("open sm");
        let mut sm = store.data.write().await;

        let mut volume = VolumeConfig {
            id: VolumeId::new("default".to_string()),
            name: "default".to_string(),
            chunk_size_bytes: 16 * 1024 * 1024,
            replication: TierReplication { hot: 0, warm: 0, cold: 1, tape: 0 },
            mandatory_replicas: TierReplication { hot: 0, warm: 0, cold: 1, tape: 0 },
            cache_replicas: CacheReplication::default(),
            anchor: None,
            spill: dtfs_common::types::SpillPolicy::default(),
            version: 0,
            anchor_datacenter: None,
            write_safety: WriteSafety {
                min_replicas_before_ack: 1,
                write_timeout_secs: 30,
            },
        };
        apply_request(
            &mut sm,
            &mk_log_id(1),
            &ClientRequest::UpsertVolume { now_ms: 0, config: volume.clone(), expected_version: None },
        );

        volume.chunk_size_bytes = 4 * 1024 * 1024;
        apply_request(
            &mut sm,
            &mk_log_id(2),
            &ClientRequest::UpsertVolume { now_ms: 0, config: volume.clone(), expected_version: None },
        );

        apply_request(
            &mut sm,
            &mk_log_id(3),
            &ClientRequest::CreateFile { now_ms: 0,
                path: "/post-resize.bin".to_string(),
                volume_id: "default".to_string(),
                chunk_size: 4 * 1024 * 1024,
            },
        );

        let file = sm.files.get("/post-resize.bin").expect("file persisted");
        assert_eq!(
            file.chunk_size,
            4 * 1024 * 1024,
            "post-resize file must use the NEW chunk_size_bytes"
        );
    }
}

#[cfg(test)]
mod posix_path_regression {
    use super::*;

    fn mk_log_id(idx: u64) -> LogId {
        use openraft::vote::RaftLeaderId;
        let leader = openraft::impls::leader_id_adv::LeaderId::<u64, u64>::new(0, 0);
        openraft::LogId::new(leader, idx)
    }

    /// CF-8 — TriggerScrub apply records the request; a later trigger
    /// replaces it (replacement is the only cleanup the design needs).
    #[test]
    fn cf8_trigger_scrub_sets_and_replaces_pending() {
        let mut sm = StateMachineData::default();
        assert!(sm.pending_scrub.is_none());
        apply_request(
            &mut sm,
            &mk_log_id(1),
            &ClientRequest::TriggerScrub {
                id: "t1".into(),
                level: "quick".into(),
                requested_at_ms: 111,
            },
        );
        let p = sm.pending_scrub.clone().expect("pending set");
        assert_eq!(
            (p.id.as_str(), p.level.as_str(), p.requested_at_ms),
            ("t1", "quick", 111)
        );
        apply_request(
            &mut sm,
            &mk_log_id(2),
            &ClientRequest::TriggerScrub {
                id: "t2".into(),
                level: "deep".into(),
                requested_at_ms: 222,
            },
        );
        assert_eq!(sm.pending_scrub.expect("replaced").id, "t2");
    }

    /// CF-8 / § A.3.1 — a snapshot written before `pending_scrub`
    /// existed must load with the field defaulting to None.
    #[test]
    fn cf8_pending_scrub_old_snapshot_defaults_none() {
        let sm = StateMachineData::default();
        let mut v = serde_json::to_value(&sm).expect("serialize");
        v.as_object_mut().unwrap().remove("pending_scrub"); // pre-CF-8 shape
        let restored: StateMachineData =
            serde_json::from_value(v).expect("old snapshot loads");
        assert!(restored.pending_scrub.is_none());
    }

    /// CO-3-1 — CreateFile stamps a deterministic logical timestamp
    /// (`log_id.index`) into the new entry + its audit row, NOT each
    /// replica's wall-clock, so the same log entry yields byte-identical
    /// state on every replica and across replay.
    #[test]
    fn co_3_1_create_file_timestamps_are_log_index_deterministic() {
        let mut sm = StateMachineData::default();
        apply_request(
            &mut sm,
            &mk_log_id(7),
            &ClientRequest::CreateFile { now_ms: 0,
                path: "/det.bin".into(),
                volume_id: "default".into(),
                chunk_size: 1024,
            },
        );
        let meta = sm.files.get("/det.bin").expect("created");
        assert_eq!(meta.created_at_ms, 7);
        assert_eq!(meta.updated_at_ms, 7);
        let entry = sm.audit_log.back().expect("audit pushed");
        assert_eq!(entry.timestamp_ms, 7);
        assert_eq!(entry.log_index, 7);
    }

    /// Bug CD — `ReleaseLeasesByHost` must reap only EXPIRED host leases. A
    /// live concurrent FUSE client (same host, different pid, lease still
    /// valid because it renews at half-life) MUST keep its lease; only the
    /// orphan (expired) lease is dropped. Pre-CD the apply dropped every
    /// non-caller lease on the host and nuked the live writer mid-write.
    #[test]
    fn bug_cd_release_by_host_spares_live_peer() {
        let mut sm = StateMachineData::default();
        let now_ms = 1_000_000u64;
        // Live concurrent writer on the same host, different pid, unexpired.
        sm.write_leases.insert(
            "/media/live.mkv".into(),
            WriteLease {
                owner_id: "toro-plex:222:7".into(),
                lease_id: "live".into(),
                expires_at_ms: now_ms + 10_000,
            },
        );
        // Orphan from a crashed prior process on the same host: expired.
        sm.write_leases.insert(
            "/media/orphan.mkv".into(),
            WriteLease {
                owner_id: "toro-plex:111:3".into(),
                lease_id: "orphan".into(),
                expires_at_ms: now_ms - 1,
            },
        );

        let resp = apply_request(
            &mut sm,
            &mk_log_id(1),
            &ClientRequest::ReleaseLeasesByHost {
                hostname_prefix: "toro-plex".into(),
                except_pid: 999, // the caller's own (new) pid; matches neither
                now_ms,
            },
        );

        assert!(
            matches!(resp, ClientResponse::WriteLeasesReleased { count: 1 }),
            "exactly one (expired orphan) lease should be released, got {resp:?}"
        );
        assert!(
            sm.write_leases.contains_key("/media/live.mkv"),
            "live concurrent writer's lease must NOT be dropped"
        );
        assert!(
            !sm.write_leases.contains_key("/media/orphan.mkv"),
            "expired orphan lease must be reaped"
        );
    }

    /// Bug A — `mkdir("/foo")` arrives at the state machine as
    /// `CreateFile { path: "/foo/" }`. After apply, the directory MUST be
    /// looked-up-able under the canonical key `"/foo"` and MUST advertise
    /// itself as a directory.
    #[test]
    fn bug_a_mkdir_trailing_slash_is_lookup_able_without_slash() {
        let mut sm = StateMachineData::default();
        apply_request(
            &mut sm,
            &mk_log_id(1),
            &ClientRequest::CreateFile { now_ms: 0,
                path: "/corpus/".into(),
                volume_id: "default".into(),
                chunk_size: 128 * 1024 * 1024,
            },
        );

        // Canonical lookup key must exist (no trailing slash).
        assert!(
            sm.files.contains_key("/corpus"),
            "directory must be findable by canonical path /corpus (got keys: {:?})",
            sm.files.keys().collect::<Vec<_>>(),
        );
        let meta = sm.files.get("/corpus").expect("just asserted");
        assert!(
            meta.is_directory,
            "FileMetadata for an empty mkdir must set is_directory=true"
        );
        assert!(
            meta.chunks.is_empty(),
            "directory entry has no chunks"
        );
    }

    /// Bug A (corollary) — a regular file create must NOT be marked as a
    /// directory, even though it also has empty chunks at create time.
    #[test]
    fn bug_a_regular_create_is_not_a_directory() {
        let mut sm = StateMachineData::default();
        apply_request(
            &mut sm,
            &mk_log_id(2),
            &ClientRequest::CreateFile { now_ms: 0,
                path: "/foo/bar.txt".into(),
                volume_id: "default".into(),
                chunk_size: 1024,
            },
        );
        let meta = sm
            .files
            .get("/foo/bar.txt")
            .expect("file must exist at its canonical path");
        assert!(
            !meta.is_directory,
            "regular file must not be tagged as a directory"
        );
    }

    /// Bug B — readdir on "/foo" should be lookup-able once any file
    /// exists under that prefix, even if no explicit mkdir was ever done.
    /// This models rsync's pattern: it writes ".plexmatch" directly under
    /// "/series/" without an mkdir, then tries to mkstemp another file in
    /// the same "directory" and the parent lookup fails.
    ///
    /// The state machine exposes a `lookup_path` helper (added by the fix)
    /// that returns Some(...) for canonical paths AND for synthetic
    /// directories implied by existing file paths.
    #[test]
    fn bug_b_synthetic_directory_is_lookup_able() {
        let mut sm = StateMachineData::default();
        apply_request(
            &mut sm,
            &mk_log_id(3),
            &ClientRequest::CreateFile { now_ms: 0,
                path: "/series/episode-01.mkv".into(),
                volume_id: "default".into(),
                chunk_size: 128 * 1024 * 1024,
            },
        );

        // The episode file itself looks up fine.
        let f = sm
            .lookup_path("/series/episode-01.mkv")
            .expect("explicit file path looks up");
        assert!(!f.is_directory);

        // The implicit parent directory MUST also be lookup-able.
        let dir = sm
            .lookup_path("/series")
            .expect("synthetic parent directory must lookup, but it returned None");
        assert!(
            dir.is_directory,
            "synthetic parent must report is_directory=true"
        );
    }

    /// Bug C — rename of a freshly-created file (with chunks already
    /// recorded via UpdateChunkLocations) must atomically move the
    /// metadata to the new key, and the old key must no longer be present.
    #[test]
    fn bug_c_rename_after_create_moves_metadata_to_new_key() {
        let mut sm = StateMachineData::default();
        // Step 1: rsync mkstemp -- create the temp file.
        apply_request(
            &mut sm,
            &mk_log_id(4),
            &ClientRequest::CreateFile { now_ms: 0,
                path: "/movies/.foo.GQhlto".into(),
                volume_id: "default".into(),
                chunk_size: 128 * 1024 * 1024,
            },
        );
        // Step 2: rsync writes data; chunk locations land via UpdateChunkLocations.
        seed_lease(&mut sm, "/movies/.foo.GQhlto");
        apply_request(
            &mut sm,
            &mk_log_id(5),
            &ClientRequest::UpdateChunkLocations { now_ms: 0,
                path: "/movies/.foo.GQhlto".into(),
                chunks: vec![ChunkLocationEntry {
                    chunk_index: 0,
                    chunk_hash: "abc123".into(),
                    node_id: "10".into(),
                    disk_id: 0,
                    size_bytes: 4096,
                }],
                lease_id: "seed".into(),
            },
        );
        // Sanity: the temp file is fully formed.
        let before = sm
            .files
            .get("/movies/.foo.GQhlto")
            .expect("temp file present before rename");
        assert_eq!(before.chunks.len(), 1);

        // Step 3: rsync renames temp -> final.
        let resp = apply_request(
            &mut sm,
            &mk_log_id(6),
            &ClientRequest::RenameFile { now_ms: 0,
                from: "/movies/.foo.GQhlto".into(),
                to: "/movies/foo.mkv".into(),
            },
        );
        assert!(
            matches!(resp, ClientResponse::Success),
            "rename must succeed for a freshly-created+written file"
        );

        // Old key is gone; new key exists with the same chunks; the
        // FileMetadata.path field reflects the new name.
        assert!(
            !sm.files.contains_key("/movies/.foo.GQhlto"),
            "old key must be removed after rename"
        );
        let after = sm
            .files
            .get("/movies/foo.mkv")
            .expect("new key must exist after rename");
        assert_eq!(after.path, "/movies/foo.mkv");
        assert_eq!(
            after.chunks.len(),
            1,
            "chunk locations carried over by rename"
        );
        assert_eq!(after.chunks[0].chunk_hash, "abc123");

        // And the parent directory is still lookup-able (Bug B linkage —
        // synthetic dirs survive a rename of any child).
        let parent = sm
            .lookup_path("/movies")
            .expect("parent dir lookup after rename");
        assert!(parent.is_directory);
    }

    /// DeregisterNode integrity guard: a node that still holds chunk replicas
    /// MUST NOT be removed (would dangle its location entries and silently
    /// lower the replication factor). Once drained it deregisters cleanly;
    /// an unknown node is a no-op Error.
    #[test]
    fn deregister_node_refuses_while_holding_replicas_then_succeeds_when_drained() {
        let mut sm = StateMachineData::default();
        apply_request(
            &mut sm,
            &mk_log_id(1),
            &ClientRequest::RegisterNode { now_ms: 0, node_id: 10, info: mk_data_node_with_disk(0) },
        );
        apply_request(
            &mut sm,
            &mk_log_id(2),
            &ClientRequest::CreateFile { now_ms: 0,
                path: "/f.bin".into(),
                volume_id: "default".into(),
                chunk_size: 4 * 1024 * 1024,
            },
        );
        seed_lease(&mut sm, "/f.bin");
        apply_request(
            &mut sm,
            &mk_log_id(3),
            &ClientRequest::UpdateChunkLocations { now_ms: 0,
                path: "/f.bin".into(),
                chunks: vec![ChunkLocationEntry {
                    chunk_index: 0,
                    chunk_hash: "h0".into(),
                    node_id: "10".into(),
                    disk_id: 0,
                    size_bytes: 4096,
                }],
                lease_id: "seed".into(),
            },
        );

        // Refused while node 10 still holds the replica.
        let refused =
            apply_request(&mut sm, &mk_log_id(4), &ClientRequest::DeregisterNode { now_ms: 0, node_id: 10 });
        match refused {
            ClientResponse::Error { message } => {
                assert!(message.contains("still holds chunk replicas"), "got: {message}")
            }
            other => panic!("expected Error while holding replicas, got {other:?}"),
        }
        assert!(sm.nodes.contains_key(&10), "node must remain while it holds replicas");

        // Drain the replica (RemoveCorrupt keeps a sole row, so clear it
        // directly), then deregister succeeds and removes the node.
        sm.files.values_mut().for_each(|f| f.chunks.clear());
        let ok =
            apply_request(&mut sm, &mk_log_id(6), &ClientRequest::DeregisterNode { now_ms: 0, node_id: 10 });
        assert!(matches!(ok, ClientResponse::Success), "drained node must deregister");
        assert!(!sm.nodes.contains_key(&10), "node removed after deregister");

        // Unknown node -> Error (idempotent safety, no panic).
        let unknown =
            apply_request(&mut sm, &mk_log_id(7), &ClientRequest::DeregisterNode { now_ms: 0, node_id: 99 });
        assert!(matches!(unknown, ClientResponse::Error { .. }), "unknown node deregister is Error");
    }

    /// Bug E — `list_directory_entries` must report a path as a directory
    /// when EITHER an explicit mkdir entry OR a child file path exists,
    /// independent of BTreeMap iteration order.
    ///
    /// Symptom on the lab: after mkdir + writing files inside, `ls -l`
    /// showed the directory as a regular file with size equal to the sum
    /// of children — `find` saw it as a non-directory and never recursed.
    #[test]
    fn bug_e_dir_with_mkdir_marker_and_children_is_a_directory_with_4k_size() {
        let mut sm = StateMachineData::default();
        // 1. mkdir /series  (FUSE sends with trailing slash)
        apply_request(
            &mut sm,
            &mk_log_id(10),
            &ClientRequest::CreateFile { now_ms: 0,
                path: "/series/".into(),
                volume_id: "default".into(),
                chunk_size: 128 * 1024 * 1024,
            },
        );
        // 2. write /series/episode-01.mkv (10 GB worth of "chunks")
        apply_request(
            &mut sm,
            &mk_log_id(11),
            &ClientRequest::CreateFile { now_ms: 0,
                path: "/series/episode-01.mkv".into(),
                volume_id: "default".into(),
                chunk_size: 128 * 1024 * 1024,
            },
        );
        seed_lease(&mut sm, "/series/episode-01.mkv");
        apply_request(
            &mut sm,
            &mk_log_id(12),
            &ClientRequest::UpdateChunkLocations { now_ms: 0,
                path: "/series/episode-01.mkv".into(),
                chunks: vec![ChunkLocationEntry {
                    chunk_index: 0,
                    chunk_hash: "h".into(),
                    node_id: "10".into(),
                    disk_id: 0,
                    // 10 GB single chunk for the test
                    size_bytes: 10 * 1024 * 1024 * 1024,
                }],
                lease_id: "seed".into(),
            },
        );

        // List from `/`: expect ONE entry named "series" that's a
        // directory with size 4096.
        let entries = sm.list_directory_entries("/");
        assert_eq!(
            entries.len(),
            1,
            "ls / should return a single entry, got {:?}",
            entries
        );
        let e = &entries[0];
        assert_eq!(e.name, "series");
        assert!(
            e.is_directory,
            "directory must be reported as is_directory=true, got {:?}",
            e
        );
        assert_eq!(
            e.size_bytes, 4096,
            "directory size must be 4096 (one block), not the sum of children — got {} (= {} MB)",
            e.size_bytes,
            e.size_bytes / 1_048_576
        );

        // And `ls /series/` should return the episode as a regular file
        // with its real size.
        let inside = sm.list_directory_entries("/series");
        let episode = inside
            .iter()
            .find(|x| x.name == "episode-01.mkv")
            .expect("episode must be present in /series listing");
        assert!(!episode.is_directory);
        assert_eq!(episode.size_bytes, 10 * 1024 * 1024 * 1024);
    }

    /// Bug E (synthetic dir variant) — even without an explicit mkdir,
    /// a directory implied by a child file path must list as a directory
    /// from its parent. (Catches the case where rsync writes files
    /// straight into a folder without ever calling mkdir.)
    #[test]
    fn bug_e_synthetic_dir_lists_as_directory() {
        let mut sm = StateMachineData::default();
        apply_request(
            &mut sm,
            &mk_log_id(20),
            &ClientRequest::CreateFile { now_ms: 0,
                path: "/movies/foo.mkv".into(),
                volume_id: "default".into(),
                chunk_size: 1024,
            },
        );
        let entries = sm.list_directory_entries("/");
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].name, "movies");
        assert!(entries[0].is_directory);
        assert_eq!(entries[0].size_bytes, 4096);
    }

    // ----------- Bug O — scrub auto-repair ---------------------------

    /// When the data node reports chunks as corrupt via
    /// `RemoveCorruptChunkReplicas`, the matching (node_id, chunk_hash)
    /// entries are dropped from every file's chunk list. The
    /// scheduler's `check_under_replicated` pass then sees the file
    /// has fewer than RF replicas and queues a copy from a healthy
    /// peer.
    #[test]
    fn bug_o_remove_corrupt_chunk_replicas_drops_matching_entries() {
        let mut sm = StateMachineData::default();
        sm.files.insert(
            "/a.bin".into(),
            FileMetadata {
                path: "/a.bin".into(),
                volume_id: "default".into(),
                chunk_size: 1024,
                chunks: vec![
                    ChunkLocationEntry {
                        chunk_index: 0,
                        chunk_hash: "corrupt-hash".into(),
                        node_id: "10".into(),
                        disk_id: 0,
                        size_bytes: 1024,
                    },
                    ChunkLocationEntry {
                        chunk_index: 0,
                        chunk_hash: "corrupt-hash".into(),
                        node_id: "11".into(),
                        disk_id: 0,
                        size_bytes: 1024,
                    },
                ],
                created_at_ms: 1,
                updated_at_ms: 1,
                symlink_target: None,
                logical_size: None,
                is_directory: false,
                cache_cooldown_until_ms: std::collections::BTreeMap::new(), ever_committed: None,
            },
        );

        apply_request(
            &mut sm,
            &mk_log_id(99),
            &ClientRequest::RemoveCorruptChunkReplicas { now_ms: 0,
                node_id: "10".into(),
                chunk_hashes: vec!["corrupt-hash".into()],
                disk_id: legacy_disk_id(),
            },
        );

        // Node 10's replica is gone; node 11's replica survives.
        let a = sm.files.get("/a.bin").unwrap();
        assert_eq!(a.chunks.len(), 1);
        assert_eq!(a.chunks[0].node_id, "11");
        assert_eq!(a.chunks[0].chunk_hash, "corrupt-hash");
    }

    fn corrupt_disk_fixture() -> StateMachineData {
        let mut sm = StateMachineData::default();
        let mk = |node: &str, disk: u32| ChunkLocationEntry {
            chunk_index: 0,
            chunk_hash: "h".into(),
            node_id: node.into(),
            disk_id: disk,
            size_bytes: 1024,
        };
        sm.files.insert(
            "/d.bin".into(),
            FileMetadata {
                path: "/d.bin".into(),
                volume_id: "default".into(),
                chunk_size: 1024,
                chunks: vec![mk("10", 0), mk("10", 2), mk("11", 2)],
                created_at_ms: 1,
                updated_at_ms: 1,
                symlink_target: None,
                logical_size: None,
                is_directory: false,
                cache_cooldown_until_ms: std::collections::BTreeMap::new(), ever_committed: None,
            },
        );
        sm
    }

    /// A corrupt copy on (node 10, disk 2) must not drop the good copy on
    /// (node 10, disk 0) nor node 11's copy on disk 2.
    #[test]
    fn remove_corrupt_with_disk_id_drops_only_that_disk() {
        let mut sm = corrupt_disk_fixture();
        apply_request(
            &mut sm,
            &mk_log_id(100),
            &ClientRequest::RemoveCorruptChunkReplicas {
                now_ms: 0,
                node_id: "10".into(),
                chunk_hashes: vec!["h".into()],
                disk_id: 2,
            },
        );
        let got: Vec<(String, u32)> = sm.files["/d.bin"]
            .chunks
            .iter()
            .map(|c| (c.node_id.clone(), c.disk_id))
            .collect();
        assert_eq!(got, vec![("10".to_string(), 0), ("11".to_string(), 2)]);
    }

    /// Legacy sentinel keeps the node-wide sweep (both of node 10's copies).
    #[test]
    fn remove_corrupt_without_disk_id_sweeps_whole_node() {
        let mut sm = corrupt_disk_fixture();
        apply_request(
            &mut sm,
            &mk_log_id(100),
            &ClientRequest::RemoveCorruptChunkReplicas {
                now_ms: 0,
                node_id: "10".into(),
                chunk_hashes: vec!["h".into()],
                disk_id: legacy_disk_id(),
            },
        );
        let got: Vec<&str> = sm.files["/d.bin"].chunks.iter().map(|c| c.node_id.as_str()).collect();
        assert_eq!(got, vec!["11"]);
    }

    fn upsert(sm: &mut StateMachineData, idx: u64, v: VolumeConfig, exp: Option<u64>) -> ClientResponse {
        apply_request(
            sm,
            &mk_log_id(idx),
            &ClientRequest::UpsertVolume { now_ms: 0, config: v, expected_version: exp },
        )
    }

    /// Volume CAS: apply owns `version` (bumped per upsert, wire value ignored);
    /// a stale `expected_version` is refused and leaves the stored volume alone.
    #[test]
    fn upsert_volume_cas_bumps_and_rejects_stale() {
        let mut sm = StateMachineData::default();
        let mut v = VolumeConfig::default();
        v.version = 99; // ignored
        assert!(matches!(upsert(&mut sm, 1, v.clone(), None), ClientResponse::Success));
        assert_eq!(sm.volumes["default"].version, 1);

        // Fresh read at version 1 wins, bumping to 2.
        v.name = "a".into();
        assert!(matches!(upsert(&mut sm, 2, v.clone(), Some(1)), ClientResponse::Success));
        assert_eq!(sm.volumes["default"].version, 2);

        // A concurrent writer that also read version 1 is refused.
        v.name = "b".into();
        match upsert(&mut sm, 3, v.clone(), Some(1)) {
            ClientResponse::Error { message } => assert_eq!(
                message,
                "volume version conflict on default: expected 1, current 2"
            ),
            other => panic!("expected conflict, got {other:?}"),
        }
        assert_eq!(sm.volumes["default"].name, "a");
        assert_eq!(sm.volumes["default"].version, 2);

        // Unconditional upsert still works and bumps.
        assert!(matches!(upsert(&mut sm, 4, v, None), ClientResponse::Success));
        assert_eq!(sm.volumes["default"].version, 3);
    }

    /// A missing volume counts as version 0: Some(0) creates, Some(1) is refused.
    #[test]
    fn upsert_volume_cas_missing_volume_is_version_zero() {
        let mut sm = StateMachineData::default();
        let v = VolumeConfig::default();
        assert!(matches!(upsert(&mut sm, 1, v.clone(), Some(1)), ClientResponse::Error { .. }));
        assert!(sm.volumes.is_empty());
        assert!(matches!(upsert(&mut sm, 2, v, Some(0)), ClientResponse::Success));
        assert_eq!(sm.volumes["default"].version, 1);
    }

    /// Audit #11 — SweepExpiredLeases also reaps expired POSIX byte-range
    /// locks (crashed clients), drops emptied tables, keeps live locks.
    #[test]
    fn sweep_expired_leases_reaps_expired_posix_locks() {
        let mut sm = StateMachineData::default();
        let lock = |path: &str, owner: &str, start: u64, lease_ms: u64| ClientRequest::AcquireLock {
            path: path.into(), owner_id: owner.into(), start, end: start + 10,
            write: true, lease_ms, now_ms: 1_000,
        };
        apply_request(&mut sm, &mk_log_id(1), &lock("/dead", "crashed", 0, 1_000));
        apply_request(&mut sm, &mk_log_id(2), &lock("/mixed", "crashed", 0, 1_000));
        apply_request(&mut sm, &mk_log_id(3), &lock("/mixed", "live", 20, 60_000));
        // Boundary: expires_at_ms == now_ms is expired (matches AcquireLock).
        apply_request(&mut sm, &mk_log_id(4), &ClientRequest::SweepExpiredLeases { now_ms: 2_000, sweep_posix_locks: true });
        assert!(!sm.posix_locks.contains_key("/dead"), "{:?}", sm.posix_locks);
        let mixed = sm.posix_locks.get("/mixed").expect("live lock's table kept");
        assert_eq!(mixed.iter().map(|l| l.owner_id.as_str()).collect::<Vec<_>>(), ["live"]);
    }

    /// Bug P — `UpdateChunkLocations` MUST reject a commit whose
    /// `lease_id` doesn't match the currently-held lease for the
    /// path. Pre-fix a stale writer (whose lease the manager had
    /// already GC'd) could still commit its chunks alongside a fresh
    /// writer's, interleaving the file's bytes per chunk_index.
    #[test]
    fn bug_p_update_chunk_locations_rejects_stale_lease_id() {
        let mut sm = StateMachineData::default();
        // 1. Create the file.
        apply_request(
            &mut sm,
            &mk_log_id(1),
            &ClientRequest::CreateFile { now_ms: 0,
                path: "/x.bin".into(),
                volume_id: "default".into(),
                chunk_size: 1024,
            },
        );
        // 2. Writer-A acquires the lease.
        apply_request(
            &mut sm,
            &mk_log_id(2),
            &ClientRequest::AcquireWriteLease {
                path: "/x.bin".into(),
                owner_id: "host-a:pid-1:fh-9".into(),
                lease_ms: 15_000,
                now_ms: 1_000_000,
                proposed_lease_id: "lease-a-uuid".into(),
            },
        );
        let lease_a = sm
            .write_leases
            .get("/x.bin")
            .map(|l| l.lease_id.clone())
            .expect("writer-A holds the lease");

        // 3. Writer-A's lease is reaped (simulate GC sweep at lease+1).
        apply_request(
            &mut sm,
            &mk_log_id(3),
            &ClientRequest::SweepExpiredLeases {
                now_ms: 1_000_000 + 16_000,
                sweep_posix_locks: true,
            },
        );
        assert!(sm.write_leases.get("/x.bin").is_none());

        // 4. Writer-B acquires fresh.
        apply_request(
            &mut sm,
            &mk_log_id(4),
            &ClientRequest::AcquireWriteLease {
                path: "/x.bin".into(),
                owner_id: "host-b:pid-2:fh-1".into(),
                lease_ms: 15_000,
                now_ms: 1_000_000 + 20_000,
                proposed_lease_id: "lease-b-uuid".into(),
            },
        );
        let lease_b = sm
            .write_leases
            .get("/x.bin")
            .map(|l| l.lease_id.clone())
            .expect("writer-B holds the lease now");
        assert_ne!(lease_a, lease_b);

        // 5. Writer-A's belated commit must be REJECTED.
        let resp = apply_request(
            &mut sm,
            &mk_log_id(5),
            &ClientRequest::UpdateChunkLocations { now_ms: 0,
                path: "/x.bin".into(),
                chunks: vec![ChunkLocationEntry {
                    chunk_index: 0,
                    chunk_hash: "stale-bytes".into(),
                    node_id: "10".into(),
                    disk_id: 0,
                    size_bytes: 4096,
                }],
                lease_id: lease_a.clone(),
            },
        );
        assert!(
            matches!(resp, ClientResponse::WriteLeaseConflict { .. }),
            "stale lease must be rejected with WriteLeaseConflict; got {:?}",
            resp
        );
        // And the file's chunks must NOT have been mutated by A's stale commit.
        let f = sm.files.get("/x.bin").unwrap();
        assert!(
            f.chunks.is_empty(),
            "file must have no chunks committed by the stale writer"
        );

        // 6. Writer-B's commit with the right lease_id succeeds.
        let resp = apply_request(
            &mut sm,
            &mk_log_id(6),
            &ClientRequest::UpdateChunkLocations { now_ms: 0,
                path: "/x.bin".into(),
                chunks: vec![ChunkLocationEntry {
                    chunk_index: 0,
                    chunk_hash: "fresh-bytes".into(),
                    node_id: "10".into(),
                    disk_id: 0,
                    size_bytes: 4096,
                }],
                lease_id: lease_b,
            },
        );
        assert!(matches!(resp, ClientResponse::Success));
        let f = sm.files.get("/x.bin").unwrap();
        assert_eq!(f.chunks.len(), 1);
        assert_eq!(f.chunks[0].chunk_hash, "fresh-bytes");
    }

    /// Bug P — empty lease_id preserves legacy callers (system-driven
    /// re-replication, older clients). The commit succeeds without
    /// lease verification.
    #[test]
    fn bug_p_empty_lease_id_skips_verification_legacy_compat() {
        let mut sm = StateMachineData::default();
        apply_request(
            &mut sm,
            &mk_log_id(1),
            &ClientRequest::CreateFile { now_ms: 0,
                path: "/y.bin".into(),
                volume_id: "default".into(),
                chunk_size: 1024,
            },
        );
        let resp = apply_request(
            &mut sm,
            &mk_log_id(2),
            &ClientRequest::UpdateChunkLocations { now_ms: 0,
                path: "/y.bin".into(),
                chunks: vec![ChunkLocationEntry {
                    chunk_index: 0,
                    chunk_hash: "legacy".into(),
                    node_id: "10".into(),
                    disk_id: 0,
                    size_bytes: 1024,
                }],
                lease_id: String::new(),
            },
        );
        // Lease-less writes can no longer introduce an index (stale-writer
        // protection); the client sees an error rather than silent loss.
        assert!(
            matches!(resp, ClientResponse::Error { .. }),
            "lease-less initial add must be refused, got {resp:?}"
        );
        assert!(sm.files.get("/y.bin").unwrap().chunks.is_empty());
    }

    /// Bug O — unrelated chunks on the same node MUST be preserved.
    #[test]
    fn bug_o_remove_does_not_drop_unrelated_chunks_on_same_node() {
        let mut sm = StateMachineData::default();
        sm.files.insert(
            "/c.bin".into(),
            FileMetadata {
                path: "/c.bin".into(),
                volume_id: "default".into(),
                chunk_size: 1024,
                chunks: vec![
                    ChunkLocationEntry {
                        chunk_index: 0,
                        chunk_hash: "h-bad".into(),
                        node_id: "20".into(),
                        disk_id: 0,
                        size_bytes: 1024,
                    },
                    ChunkLocationEntry {
                        chunk_index: 0,
                        chunk_hash: "h-bad".into(),
                        node_id: "21".into(),
                        disk_id: 0,
                        size_bytes: 1024,
                    },
                    ChunkLocationEntry {
                        chunk_index: 1,
                        chunk_hash: "h-good".into(),
                        node_id: "20".into(),
                        disk_id: 0,
                        size_bytes: 1024,
                    },
                ],
                created_at_ms: 1,
                updated_at_ms: 1,
                symlink_target: None,
                logical_size: None,
                is_directory: false,
                cache_cooldown_until_ms: std::collections::BTreeMap::new(), ever_committed: None,
            },
        );
        apply_request(
            &mut sm,
            &mk_log_id(100),
            &ClientRequest::RemoveCorruptChunkReplicas { now_ms: 0,
                node_id: "20".into(),
                chunk_hashes: vec!["h-bad".into()],
                disk_id: legacy_disk_id(),
            },
        );
        let c = sm.files.get("/c.bin").unwrap();
        // Node 20's bad row is gone; node 21's row for the same index and
        // node 20's unrelated good chunk survive.
        assert_eq!(c.chunks.len(), 2);
        assert!(c.chunks.iter().all(|e| !(e.node_id == "20" && e.chunk_hash == "h-bad")));
        assert!(c.chunks.iter().any(|e| e.chunk_hash == "h-good"));
    }

    // ---- Bug AH — replicated audit log fan-out ----

    /// `AppendAudit` must push a fully-populated entry onto `sm.audit_log`
    /// with structured fields preserved and a synthesised `summary` for
    /// legacy display.
    #[test]
    fn bug_ah_append_audit_pushes_entry_with_structured_fields() {
        let mut sm = StateMachineData::default();
        let before = sm.audit_log.len();

        let resp = apply_request(
            &mut sm,
            &mk_log_id(7),
            &ClientRequest::AppendAudit {
                actor: "scheduler".into(),
                action: "replication.complete".into(),
                target: "chunk=abc src=10 dst=11".into(),
                result: "success".into(),
                origin_manager: "mgr02".into(),
                timestamp_ms: 1_779_000_000_000,
            },
        );
        assert!(matches!(resp, ClientResponse::Success));
        assert_eq!(sm.audit_log.len(), before + 1);

        let entry = sm.audit_log.back().unwrap();
        assert_eq!(entry.actor, "scheduler");
        assert_eq!(entry.action, "replication.complete");
        assert_eq!(entry.target, "chunk=abc src=10 dst=11");
        assert_eq!(entry.result, "success");
        assert_eq!(entry.origin_manager, "mgr02");
        assert_eq!(entry.timestamp_ms, 1_779_000_000_000);
        // Synthesised summary preserves the legacy display path.
        assert!(entry.summary.contains("replication.complete"));
        assert!(entry.summary.contains("chunk=abc src=10 dst=11"));
        assert!(entry.summary.contains("mgr02"));
        // log_index is the Raft log index at apply time.
        assert_eq!(entry.log_index, 7);
    }

    /// `AppendAudit` with an empty target must still produce a sensible
    /// summary (no trailing `": "`). Covers system events like
    /// `manager.start` which carry only an actor/action/result.
    #[test]
    fn bug_ah_append_audit_targetless_summary_is_clean() {
        let mut sm = StateMachineData::default();
        apply_request(
            &mut sm,
            &mk_log_id(8),
            &ClientRequest::AppendAudit {
                actor: "system".into(),
                action: "manager.start".into(),
                target: String::new(),
                result: "success".into(),
                origin_manager: "mgr01".into(),
                timestamp_ms: 100,
            },
        );
        let entry = sm.audit_log.back().unwrap();
        assert_eq!(entry.summary, "manager.start [mgr01]");
        assert!(entry.target.is_empty());
    }

    /// Bug AH compat — an `AuditEntry` serialised before the structured
    /// fields existed (only `log_index` + `summary` + `timestamp_ms`)
    /// must deserialise cleanly via `#[serde(default)]` on the new fields.
    /// This guards every existing snapshot on disk.
    #[test]
    fn bug_ah_audit_entry_backward_compat_deserialise() {
        let legacy = r#"{"log_index":42,"summary":"DeleteFile: /x","timestamp_ms":1234}"#;
        let entry: AuditEntry = serde_json::from_str(legacy).expect("legacy shape must deserialise");
        assert_eq!(entry.log_index, 42);
        assert_eq!(entry.summary, "DeleteFile: /x");
        assert_eq!(entry.timestamp_ms, 1234);
        // New fields default to empty strings — safe for the API
        // serializer to surface unchanged.
        assert!(entry.actor.is_empty());
        assert!(entry.action.is_empty());
        assert!(entry.target.is_empty());
        assert!(entry.result.is_empty());
        assert!(entry.origin_manager.is_empty());
    }

    /// `AppendAudit` is subject to the same `MAX_AUDIT_LOG_ENTRIES` cap
    /// as native audit pushes (CreateFile, RegisterNode, etc.) — both
    /// paths flow through `push_audit_capped`. Verifies the cap holds
    /// under a flood from the fan-out path.
    #[test]
    fn bug_ah_append_audit_respects_global_cap() {
        let mut sm = StateMachineData::default();
        // Fill close to the cap with native audit events first.
        for i in 0..(MAX_AUDIT_LOG_ENTRIES as u64) {
            apply_request(
                &mut sm,
                &mk_log_id(i + 1),
                &ClientRequest::CreateFile { now_ms: 0,
                    path: format!("/seed/{i}"),
                    volume_id: "default".into(),
                    chunk_size: 1024,
                },
            );
        }
        assert_eq!(sm.audit_log.len(), MAX_AUDIT_LOG_ENTRIES);

        // Now flood with AppendAudit — the cap must still hold.
        for i in 0..50 {
            apply_request(
                &mut sm,
                &mk_log_id(MAX_AUDIT_LOG_ENTRIES as u64 + i + 1),
                &ClientRequest::AppendAudit {
                    actor: "scheduler".into(),
                    action: "flood".into(),
                    target: format!("evt={i}"),
                    result: "success".into(),
                    origin_manager: "mgr03".into(),
                    timestamp_ms: 0,
                },
            );
        }
        assert_eq!(sm.audit_log.len(), MAX_AUDIT_LOG_ENTRIES);
        // The newest entries should be the fan-out ones (FIFO eviction
        // by `push_audit_capped`).
        let last = sm.audit_log.back().unwrap();
        assert_eq!(last.action, "flood");
        assert_eq!(last.origin_manager, "mgr03");
    }

    // ---- AV-1-3 — RemoveChunkLocation tests -----------------------

    /// Build a minimal SM with one file + one chunk and the named
    /// nodes pre-registered. Each node gets a single disk of the
    /// requested tier in the requested DC. Used by the AV-1-3 tests.
    fn sm_with_chunk(
        file_path: &str,
        chunk_index: u64,
        chunk_hash: &str,
        replicas: &[(u64, &str, dtfs_common::types::DiskTier)],
    ) -> StateMachineData {
        use dtfs_common::topology::Location;
        use dtfs_common::types::DiskStatus;

        let mut sm = StateMachineData::default();
        for (nid, dc, tier) in replicas {
            sm.nodes.insert(*nid, crate::NodeInfo {
                role: dtfs_common::types::NodeRole::Data,
                hostname: format!("node-{nid}"),
                grpc_addr: String::new(),
                location: Location::new("lab", dc, "rack-1", &format!("node-{nid}")),
                disks: vec![dtfs_common::types::DiskInfo {
                    disk_id: 0,
                    uuid: String::new(),
                    mount_path: String::new(),
                    tier: *tier,
                    total_bytes: 0,
                    used_bytes: 0,
                    status: DiskStatus::Healthy,
                }],
                total_capacity: 0,
                used_capacity: 0,
                health: dtfs_common::types::HealthStatus::Healthy,
                last_seen_ms: 0,
                total_bytes_read: 0,
                total_bytes_written: 0,
                client_ip: None,
                version: String::new(),
                build_date: String::new(),
                quarantine: None,
            });
        }
        sm.files.insert(file_path.to_string(), FileMetadata {
            path: file_path.to_string(),
            volume_id: "default".to_string(),
            chunk_size: 1024,
            chunks: replicas
                .iter()
                .map(|(nid, _, _)| ChunkLocationEntry {
                    chunk_index,
                    chunk_hash: chunk_hash.to_string(),
                    node_id: nid.to_string(),
                    disk_id: 0,
                    size_bytes: 1024,
                })
                .collect(),
            created_at_ms: 1,
            updated_at_ms: 1,
            symlink_target: None,
            logical_size: None,
            is_directory: false,
            cache_cooldown_until_ms: std::collections::BTreeMap::new(), ever_committed: None,
        });
        sm
    }

    /// REV-3-7 — the anchor survivor scan must use the same liveness rule as
    /// the scheduler's coverage counting (`live_replica_tier`). A replica on a
    /// Down node is not servable, so it cannot be the thing that makes a
    /// removal safe. Pre-fix the two disagreed: `check_under_replicated` saw a
    /// deficit and scheduled a backfill, and this scan let the rebalance drop
    /// the fresh copy again on the strength of the dead anchor — forever.
    #[test]
    fn rev_3_7_a_dead_replica_cannot_satisfy_the_anchor() {
        use dtfs_common::types::{AnchorPolicy, DiskTier, HealthStatus};
        let mut sm = sm_with_chunk("/x", 0, "h", &[
            (10, "dc3", DiskTier::Cold), // the anchor copy
            (11, "toro", DiskTier::Warm),
        ]);
        let policy = AnchorPolicy {
            datacenter: "dc3".to_string(),
            required_tier: Some(DiskTier::Cold),
        };

        // With the anchor node healthy, dropping the warm copy is fine.
        let mut healthy = sm.clone();
        let resp = apply_request(&mut healthy, &mk_log_id(1),
            &ClientRequest::RemoveChunkLocation { now_ms: 0,
                path: "/x".into(),
                chunk_index: 0,
                node_id: "11".into(),
                disk_id: 0,
                enforce_anchor: Some(policy.clone()),
            });
        assert!(matches!(resp, ClientResponse::Success));

        // Now the anchor node is Down: its replica is not servable, so the
        // same removal must be refused instead of counting on it.
        sm.nodes.get_mut(&10).unwrap().health = HealthStatus::Down;
        let resp = apply_request(&mut sm, &mk_log_id(2),
            &ClientRequest::RemoveChunkLocation { now_ms: 0,
                path: "/x".into(),
                chunk_index: 0,
                node_id: "11".into(),
                disk_id: 0,
                enforce_anchor: Some(policy.clone()),
            });
        match resp {
            ClientResponse::Error { message } => {
                assert!(message.contains("would strand anchor"), "got: {message}");
            }
            other => panic!("expected refusal while the anchor node is Down, got {other:?}"),
        }
        assert_eq!(sm.files.get("/x").unwrap().chunks.len(), 2, "nothing removed");

        // Same rule for a Failed/Removed DISK under a healthy node.
        let mut bad_disk = sm.clone();
        bad_disk.nodes.get_mut(&10).unwrap().health = HealthStatus::Healthy;
        bad_disk.nodes.get_mut(&10).unwrap().disks[0].status =
            dtfs_common::types::DiskStatus::Failed;
        let resp = apply_request(&mut bad_disk, &mk_log_id(3),
            &ClientRequest::RemoveChunkLocation { now_ms: 0,
                path: "/x".into(),
                chunk_index: 0,
                node_id: "11".into(),
                disk_id: 0,
                enforce_anchor: Some(policy),
            });
        assert!(
            matches!(resp, ClientResponse::Error { .. }),
            "a Failed anchor disk cannot authorise the removal either"
        );
    }

    /// CR-3-2/MGR-2 (defence) — the last-live-replica guard fires
    /// independent of `enforce_anchor`: two rows, one on a Down node.
    /// Removing the LIVE row (no anchor policy passed at all) must be
    /// refused because it is the only servable copy; removing the Down
    /// row is fine (it wasn't serving anything).
    #[test]
    fn cr_3_2_last_live_replica_guard_is_independent_of_anchor() {
        use dtfs_common::types::{DiskTier, HealthStatus};
        let mut sm = sm_with_chunk("/x", 0, "h", &[
            (10, "leli", DiskTier::Cold),
            (11, "toro", DiskTier::Warm),
        ]);
        sm.nodes.get_mut(&10).unwrap().health = HealthStatus::Down;

        // Removing the live (toro) row would leave zero live replicas.
        let mut sm_try_live = sm.clone();
        let resp = apply_request(&mut sm_try_live, &mk_log_id(1),
            &ClientRequest::RemoveChunkLocation { now_ms: 0,
                path: "/x".into(),
                chunk_index: 0,
                node_id: "11".into(),
                disk_id: 0,
                enforce_anchor: None,
            });
        match resp {
            ClientResponse::Error { message } => {
                assert!(message.contains("last live replica"), "got: {message}");
            }
            other => panic!("expected refusal removing the last live replica, got {other:?}"),
        }
        assert_eq!(sm_try_live.files.get("/x").unwrap().chunks.len(), 2, "nothing removed");

        // Removing the Down (leli) row is fine — it wasn't live anyway.
        let mut sm_try_dead = sm.clone();
        let resp = apply_request(&mut sm_try_dead, &mk_log_id(2),
            &ClientRequest::RemoveChunkLocation { now_ms: 0,
                path: "/x".into(),
                chunk_index: 0,
                node_id: "10".into(),
                disk_id: 0,
                enforce_anchor: None,
            });
        assert!(matches!(resp, ClientResponse::Success));
        assert_eq!(sm_try_dead.files.get("/x").unwrap().chunks.len(), 1, "dead row removed");
    }

    fn dead_req(node: &str) -> ClientRequest {
        ClientRequest::RemoveDeadNodeReplica {
            path: "/x".into(), chunk_index: 0, node_id: node.into(), disk_id: 0, now_ms: 77,
        }
    }

    /// Remove-dead-node data-loss arm: refuses a live node and a chunk that
    /// still has a live copy; removes the only (dead) copy with an audit row;
    /// idempotent on a second apply.
    #[test]
    fn remove_dead_node_replica_requires_dead_node_and_no_live_copy() {
        use dtfs_common::types::{DiskTier, HealthStatus};
        let mut sm = sm_with_chunk("/x", 0, "h", &[
            (10, "leli", DiskTier::Cold),
            (11, "toro", DiskTier::Warm),
        ]);
        // Node 10 still Healthy: refused.
        match apply_request(&mut sm, &mk_log_id(1), &dead_req("10")) {
            ClientResponse::Error { message } => assert!(message.contains("not Down/Failed"), "{message}"),
            other => panic!("expected refusal, got {other:?}"),
        }
        // Node 10 Down but node 11 still serves the chunk: refused.
        sm.nodes.get_mut(&10).unwrap().health = HealthStatus::Down;
        match apply_request(&mut sm, &mk_log_id(2), &dead_req("10")) {
            ClientResponse::Error { message } => assert!(message.contains("has a live replica"), "{message}"),
            other => panic!("expected refusal, got {other:?}"),
        }
        assert_eq!(sm.files["/x"].chunks.len(), 2, "nothing removed");
        // Both dead: the row goes, one audit row names the chunk.
        sm.nodes.get_mut(&11).unwrap().health = HealthStatus::Down;
        let audits_before = sm.audit_log.len();
        assert!(matches!(apply_request(&mut sm, &mk_log_id(3), &dead_req("10")), ClientResponse::Success));
        assert_eq!(sm.files["/x"].chunks.len(), 1);
        assert_eq!(sm.files["/x"].chunks[0].node_id, "11");
        assert_eq!(sm.files["/x"].updated_at_ms, 77);
        assert_eq!(sm.audit_log.len(), audits_before + 1);
        let a = sm.audit_log.back().unwrap();
        assert_eq!((a.action.as_str(), a.target.as_str()), ("chunk.dead_node_replica_removed", "/x#0"));
        // Idempotent.
        assert!(matches!(apply_request(&mut sm, &mk_log_id(4), &dead_req("10")), ClientResponse::Success));
        assert_eq!(sm.audit_log.len(), audits_before + 1, "no second audit row");
    }

    /// Exhaustive over every Down/Healthy mix of three replicas and every
    /// target row: the arm never reduces the number of live replicas and
    /// never removes a row of a Healthy node.
    #[test]
    fn remove_dead_node_replica_never_drops_live_copies_exhaustive() {
        use dtfs_common::types::{DiskTier, HealthStatus};
        for mask in 0u8..8 {
            for target in [10u64, 11, 12] {
                let mut sm = sm_with_chunk("/x", 0, "h", &[
                    (10, "leli", DiskTier::Cold),
                    (11, "toro", DiskTier::Warm),
                    (12, "nrb", DiskTier::Warm),
                ]);
                for (i, nid) in [10u64, 11, 12].iter().enumerate() {
                    if mask & (1 << i) != 0 {
                        sm.nodes.get_mut(nid).unwrap().health = HealthStatus::Down;
                    }
                }
                let live = |sm: &StateMachineData| {
                    sm.files["/x"].chunks.iter().filter(|c| replica_row_is_live(sm, c)).count()
                };
                let (rows_before, live_before) = (sm.files["/x"].chunks.len(), live(&sm));
                let resp = apply_request(&mut sm, &mk_log_id(1), &dead_req(&target.to_string()));
                assert_eq!(live(&sm), live_before, "mask={mask} target={target}");
                if matches!(resp, ClientResponse::Success) && sm.files["/x"].chunks.len() < rows_before {
                    let idx = [10u64, 11, 12].iter().position(|n| *n == target).unwrap();
                    assert!(mask & (1 << idx) != 0, "removed a Healthy node's row");
                    assert_eq!(live_before, 0, "removed a row while a live copy existed");
                }
            }
        }
    }

    /// Losing the last row of a chunk index moves the whole file to
    /// `/.dtfs-lost/<path>` (surviving rows kept), audits the lost indices,
    /// and leaves other files alone; a taken target gets a numeric suffix.
    #[test]
    fn remove_dead_node_replica_last_row_moves_file_to_lost() {
        use dtfs_common::types::{DiskTier, HealthStatus};
        let mut sm = sm_with_chunk("/a/x", 0, "h", &[(10, "leli", DiskTier::Cold)]);
        let mut c1 = sm.files["/a/x"].chunks[0].clone();
        c1.chunk_index = 1;
        c1.node_id = "11".into();
        sm.files.get_mut("/a/x").unwrap().chunks.push(c1);
        let other = sm.files["/a/x"].clone();
        sm.files.insert("/other".into(), FileMetadata { path: "/other".into(), ..other });
        sm.nodes.insert(11, sm.nodes[&10].clone());
        sm.nodes.get_mut(&10).unwrap().health = HealthStatus::Down;
        let req = |p: &str| ClientRequest::RemoveDeadNodeReplica {
            path: p.into(), chunk_index: 0, node_id: "10".into(), disk_id: 0, now_ms: 5,
        };
        // Occupy the first target so the suffix path is exercised.
        sm.files.insert("/.dtfs-lost/a/x".into(), FileMetadata { path: "/.dtfs-lost/a/x".into(), ..sm.files["/other"].clone() });
        assert!(matches!(apply_request(&mut sm, &mk_log_id(1), &req("/a/x")), ClientResponse::Success));
        assert!(!sm.files.contains_key("/a/x"));
        let moved = &sm.files["/.dtfs-lost/a/x.1"];
        assert_eq!(moved.path, "/.dtfs-lost/a/x.1");
        assert_eq!(moved.chunks.len(), 1, "surviving chunk row kept");
        assert_eq!(moved.chunks[0].chunk_index, 1);
        assert_eq!(moved.updated_at_ms, 5);
        assert_eq!(sm.files["/other"].chunks.len(), 2, "other file untouched");
        let a = sm.audit_log.back().unwrap();
        assert_eq!(a.action, "file.moved_to_lost");
        assert!(a.target.contains("/a/x") && a.target.contains("/.dtfs-lost/a/x.1"), "{}", a.target);
        assert!(format!("{a:?}").contains("[0]"), "lost indices audited");

        // A file already under /.dtfs-lost/ is not moved again.
        sm.nodes.get_mut(&11).unwrap().health = HealthStatus::Down;
        let again = ClientRequest::RemoveDeadNodeReplica {
            path: "/.dtfs-lost/a/x.1".into(), chunk_index: 1, node_id: "11".into(), disk_id: 0, now_ms: 6,
        };
        assert!(matches!(apply_request(&mut sm, &mk_log_id(2), &again), ClientResponse::Success));
        assert!(sm.files.contains_key("/.dtfs-lost/a/x.1"), "stays in place");
        assert!(sm.files["/.dtfs-lost/a/x.1"].chunks.is_empty());
    }

    /// The guard reports the anchor strand for a dead anchor row whose
    /// only live survivor is a non-anchor copy (planner: "anchor pending").
    #[test]
    fn remove_check_dead_anchor_row_is_refused_until_another_anchor_copy_exists() {
        use dtfs_common::types::{AnchorPolicy, DiskTier, HealthStatus};
        let mut sm = sm_with_chunk("/x", 0, "h", &[
            (10, "leli", DiskTier::Cold),
            (11, "toro", DiskTier::Warm),
        ]);
        sm.nodes.get_mut(&10).unwrap().health = HealthStatus::Down;
        let policy = AnchorPolicy { datacenter: "leli".into(), required_tier: None };
        let err = remove_chunk_location_check(&sm, "/x", 0, "10", 0, Some(&policy)).unwrap_err();
        assert!(err.contains("would strand anchor"), "{err}");
        // Without a policy the dead row may go; absent rows are a no-op.
        assert_eq!(remove_chunk_location_check(&sm, "/x", 0, "10", 0, None), Ok(true));
        assert_eq!(remove_chunk_location_check(&sm, "/x", 5, "10", 0, None), Ok(false));
    }

    /// AV-1-3 test 1 — happy path: drop one entry, others survive.
    #[test]
    fn bug_av_remove_chunk_location_drops_entry() {
        use dtfs_common::types::DiskTier;
        let mut sm = sm_with_chunk("/x", 0, "h", &[
            (10, "dc-a", DiskTier::Cold),
            (11, "dc-b", DiskTier::Warm),
        ]);
        let resp = apply_request(&mut sm, &mk_log_id(1),
            &ClientRequest::RemoveChunkLocation { now_ms: 0,
                path: "/x".into(),
                chunk_index: 0,
                node_id: "11".into(),
                disk_id: legacy_disk_id(),
                enforce_anchor: None,
            });
        assert!(matches!(resp, ClientResponse::Success));
        let f = sm.files.get("/x").unwrap();
        assert_eq!(f.chunks.len(), 1);
        assert_eq!(f.chunks[0].node_id, "10");
    }

    /// AV-1-3 test 2 — unknown file → idempotent no-op success.
    #[test]
    fn bug_av_remove_chunk_location_unknown_file_is_noop() {
        let mut sm = StateMachineData::default();
        let resp = apply_request(&mut sm, &mk_log_id(1),
            &ClientRequest::RemoveChunkLocation { now_ms: 0,
                path: "/nope".into(),
                chunk_index: 0,
                node_id: "10".into(),
                disk_id: legacy_disk_id(),
                enforce_anchor: None,
            });
        assert!(matches!(resp, ClientResponse::Success));
        assert!(sm.files.is_empty());
    }

    /// AV-1-3 test 3 — unknown (chunk_index, node) → idempotent no-op.
    #[test]
    fn bug_av_remove_chunk_location_unknown_node_is_noop() {
        use dtfs_common::types::DiskTier;
        let mut sm = sm_with_chunk("/x", 0, "h", &[(10, "dc-a", DiskTier::Cold)]);
        let resp = apply_request(&mut sm, &mk_log_id(1),
            &ClientRequest::RemoveChunkLocation { now_ms: 0,
                path: "/x".into(),
                chunk_index: 0,
                node_id: "99".into(),  // not present
                disk_id: legacy_disk_id(),
                enforce_anchor: None,
            });
        assert!(matches!(resp, ClientResponse::Success));
        let f = sm.files.get("/x").unwrap();
        assert_eq!(f.chunks.len(), 1, "no entry dropped");
    }

    /// AV-1-3 test 4 — anchor-strand protection: refuse to drop the
    /// last anchor-satisfying replica.
    #[test]
    fn bug_av_remove_chunk_location_refuses_to_strand_anchor() {
        use dtfs_common::types::{AnchorPolicy, DiskTier};
        let mut sm = sm_with_chunk("/x", 0, "h", &[
            (10, "leli", DiskTier::Cold),
            (11, "toro", DiskTier::Warm),
        ]);
        // Attempt to drop the only leli-resident replica — last anchor copy.
        let resp = apply_request(&mut sm, &mk_log_id(1),
            &ClientRequest::RemoveChunkLocation { now_ms: 0,
                path: "/x".into(),
                chunk_index: 0,
                node_id: "10".into(),
                disk_id: legacy_disk_id(),
                enforce_anchor: Some(AnchorPolicy {
                    datacenter: "leli".into(),
                    required_tier: None,  // any-tier-in-leli policy
                }),
            });
        match resp {
            ClientResponse::Error { message } => {
                assert!(message.contains("strand anchor"), "msg: {message}");
                assert!(message.contains("leli"));
            }
            other => panic!("expected Error, got {other:?}"),
        }
        // SM unchanged.
        let f = sm.files.get("/x").unwrap();
        assert_eq!(f.chunks.len(), 2);
    }

    /// AV-1-3 test 5 — tier-aware anchor: with `required_tier=Cold`,
    /// dropping a warm replica in the anchor DC is fine (cold replica
    /// elsewhere satisfies); but dropping the only cold replica IS
    /// stranded (because the remaining warm-in-leli doesn't match the
    /// required tier).
    #[test]
    fn bug_av_remove_chunk_location_anchor_tier_required_check() {
        use dtfs_common::types::{AnchorPolicy, DiskTier};
        let policy = AnchorPolicy {
            datacenter: "leli".into(),
            required_tier: Some(DiskTier::Cold),
        };

        // (a) Setup: one cold-in-leli + one warm-in-leli.
        //     Drop the warm replica → still 1 cold-in-leli → OK.
        let mut sm = sm_with_chunk("/x", 0, "h", &[
            (10, "leli", DiskTier::Cold),
            (11, "leli", DiskTier::Warm),
        ]);
        let resp = apply_request(&mut sm, &mk_log_id(1),
            &ClientRequest::RemoveChunkLocation { now_ms: 0,
                path: "/x".into(),
                chunk_index: 0,
                node_id: "11".into(), // warm
                disk_id: legacy_disk_id(),
                enforce_anchor: Some(policy.clone()),
            });
        assert!(matches!(resp, ClientResponse::Success));
        assert_eq!(sm.files.get("/x").unwrap().chunks.len(), 1);

        // (b) Same setup; drop the cold replica → only warm-in-leli
        //     remains, which doesn't satisfy required_tier=Cold → REFUSE.
        let mut sm = sm_with_chunk("/x", 0, "h", &[
            (10, "leli", DiskTier::Cold),
            (11, "leli", DiskTier::Warm),
        ]);
        let resp = apply_request(&mut sm, &mk_log_id(1),
            &ClientRequest::RemoveChunkLocation { now_ms: 0,
                path: "/x".into(),
                chunk_index: 0,
                node_id: "10".into(),  // cold
                disk_id: legacy_disk_id(),
                enforce_anchor: Some(policy),
            });
        assert!(matches!(resp, ClientResponse::Error { .. }));
        assert_eq!(sm.files.get("/x").unwrap().chunks.len(), 2, "unchanged");
    }

    /// CNF-1 — the enforce-anchor survivor check must resolve the disk by
    /// the STABLE `DiskInfo.disk_id`, not Vec position. With the cold
    /// survivor's only disk carrying disk_id=1 at Vec index 0 (reindexed
    /// disk list), dropping the warm replica is safe and must succeed
    /// (pre-fix the positional lookup missed, tier resolved to None, and
    /// the safe removal was refused).
    #[test]
    fn cnf1_remove_chunk_location_anchor_resolves_disk_by_stable_id() {
        use dtfs_common::types::{AnchorPolicy, DiskTier};
        let mut sm = sm_with_chunk("/x", 0, "h", &[
            (10, "leli", DiskTier::Cold),
            (11, "leli", DiskTier::Warm),
        ]);
        // Reindex node 10's disk list: its only disk now carries the
        // stable disk_id=1 (still at Vec index 0); the chunk entry
        // references the stable id.
        sm.nodes.get_mut(&10).unwrap().disks[0].disk_id = 1;
        for c in &mut sm.files.get_mut("/x").unwrap().chunks {
            if c.node_id == "10" {
                c.disk_id = 1;
            }
        }
        let resp = apply_request(&mut sm, &mk_log_id(1),
            &ClientRequest::RemoveChunkLocation { now_ms: 0,
                path: "/x".into(),
                chunk_index: 0,
                node_id: "11".into(), // drop warm; cold survivor anchors
                disk_id: legacy_disk_id(),
                enforce_anchor: Some(AnchorPolicy {
                    datacenter: "leli".into(),
                    required_tier: Some(DiskTier::Cold),
                }),
            });
        assert!(matches!(resp, ClientResponse::Success), "got {resp:?}");
        assert_eq!(sm.files.get("/x").unwrap().chunks.len(), 1);
    }

    // ---- AV-1-4 — SetChunkCooldown tests --------------------------

    /// AV-1-4 test 1 — single-chunk cooldown set on a file with no
    /// prior cooldown.
    #[test]
    fn bug_av_set_cooldown_single_chunk() {
        use dtfs_common::types::DiskTier;
        let mut sm = sm_with_chunk("/x", 0, "h", &[(10, "dc-a", DiskTier::Cold)]);
        apply_request(&mut sm, &mk_log_id(1),
            &ClientRequest::SetChunkCooldown {
                path: "/x".into(),
                chunk_indexes: vec![0],
                expires_at_ms: 1_000_000,
            });
        let f = sm.files.get("/x").unwrap();
        assert_eq!(f.cache_cooldown_until_ms.get(&0), Some(&1_000_000));
    }

    /// AV-1-4 test 2 — multiple chunk_indexes in one apply call.
    #[test]
    fn bug_av_set_cooldown_multi_chunk() {
        use dtfs_common::types::DiskTier;
        let mut sm = sm_with_chunk("/x", 0, "h", &[(10, "dc-a", DiskTier::Cold)]);
        apply_request(&mut sm, &mk_log_id(1),
            &ClientRequest::SetChunkCooldown {
                path: "/x".into(),
                chunk_indexes: vec![0, 1, 2, 7],
                expires_at_ms: 5_000_000,
            });
        let f = sm.files.get("/x").unwrap();
        assert_eq!(f.cache_cooldown_until_ms.len(), 4);
        for idx in [0u64, 1, 2, 7] {
            assert_eq!(f.cache_cooldown_until_ms.get(&idx), Some(&5_000_000));
        }
    }

    /// AV-1-4 test 3 — monotonic merge: a later flush with an
    /// earlier expiry does NOT regress; a later flush with a later
    /// expiry extends.
    #[test]
    fn bug_av_set_cooldown_monotonic() {
        use dtfs_common::types::DiskTier;
        let mut sm = sm_with_chunk("/x", 0, "h", &[(10, "dc-a", DiskTier::Cold)]);
        // First flush sets t=1000
        apply_request(&mut sm, &mk_log_id(1),
            &ClientRequest::SetChunkCooldown {
                path: "/x".into(),
                chunk_indexes: vec![0],
                expires_at_ms: 1000,
            });
        // Second flush with EARLIER expiry must NOT regress
        apply_request(&mut sm, &mk_log_id(2),
            &ClientRequest::SetChunkCooldown {
                path: "/x".into(),
                chunk_indexes: vec![0],
                expires_at_ms: 500,
            });
        assert_eq!(sm.files.get("/x").unwrap().cache_cooldown_until_ms.get(&0), Some(&1000));
        // Third flush with later expiry extends
        apply_request(&mut sm, &mk_log_id(3),
            &ClientRequest::SetChunkCooldown {
                path: "/x".into(),
                chunk_indexes: vec![0],
                expires_at_ms: 2000,
            });
        assert_eq!(sm.files.get("/x").unwrap().cache_cooldown_until_ms.get(&0), Some(&2000));
        // expires_at_ms = 0 clears.
        apply_request(&mut sm, &mk_log_id(4),
            &ClientRequest::SetChunkCooldown {
                path: "/x".into(),
                chunk_indexes: vec![0],
                expires_at_ms: 0,
            });
        assert!(sm.files.get("/x").unwrap().cache_cooldown_until_ms.is_empty());
    }

    /// AV-1-4 test 4 — unknown file → idempotent no-op success.
    #[test]
    fn bug_av_set_cooldown_unknown_file_is_noop() {
        let mut sm = StateMachineData::default();
        let resp = apply_request(&mut sm, &mk_log_id(1),
            &ClientRequest::SetChunkCooldown {
                path: "/nope".into(),
                chunk_indexes: vec![0],
                expires_at_ms: 1000,
            });
        assert!(matches!(resp, ClientResponse::Success));
    }

    /// AV-1-3 test 6 — any-tier anchor policy allows dropping a
    /// warm-in-anchor-DC replica as long as another replica in the
    /// anchor DC survives (any tier).
    #[test]
    fn bug_av_remove_chunk_location_anchor_any_tier_allowed() {
        use dtfs_common::types::{AnchorPolicy, DiskTier};
        let mut sm = sm_with_chunk("/x", 0, "h", &[
            (10, "leli", DiskTier::Cold),  // anchor survivor
            (11, "leli", DiskTier::Warm),  // dropping this
        ]);
        let resp = apply_request(&mut sm, &mk_log_id(1),
            &ClientRequest::RemoveChunkLocation { now_ms: 0,
                path: "/x".into(),
                chunk_index: 0,
                node_id: "11".into(),
                disk_id: legacy_disk_id(),
                enforce_anchor: Some(AnchorPolicy {
                    datacenter: "leli".into(),
                    required_tier: None,  // any tier
                }),
            });
        assert!(matches!(resp, ClientResponse::Success));
        assert_eq!(sm.files.get("/x").unwrap().chunks.len(), 1);
    }

    // ---- AV-1-2 — FileMetadata.cache_cooldown_until_ms tests ------

    /// AV-1-2 test 6 — `FileMetadata::cache_cooldown_until_ms` is
    /// `#[serde(default)]` so old snapshots (from before AV) load
    /// cleanly with an empty BTreeMap and no behaviour change.
    #[test]
    fn bug_av_filemeta_cooldown_serde_default() {
        // Old-snapshot JSON (no `cache_cooldown_until_ms` field).
        let legacy_json = r#"{
            "path": "/old.bin",
            "volume_id": "default",
            "chunk_size": 1024,
            "chunks": [],
            "created_at_ms": 100,
            "updated_at_ms": 200
        }"#;
        let f: FileMetadata = serde_json::from_str(legacy_json).expect("loads");
        assert_eq!(f.path, "/old.bin");
        assert!(f.cache_cooldown_until_ms.is_empty(),
            "missing field defaults to empty map");
    }

    /// AV-1-2 test 7 — a populated cooldown map survives ser→de
    /// round-trip with deterministic key ordering.
    #[test]
    fn bug_av_filemeta_old_snapshot_loads() {
        let mut original = FileMetadata {
            path: "/new.bin".into(),
            volume_id: "default".into(),
            chunk_size: 1024,
            chunks: Vec::new(),
            created_at_ms: 100,
            updated_at_ms: 200,
            symlink_target: None,
            logical_size: None,
            is_directory: false,
            cache_cooldown_until_ms: std::collections::BTreeMap::new(), ever_committed: None,
        };
        original.cache_cooldown_until_ms.insert(0, 1_000_000);
        original.cache_cooldown_until_ms.insert(2, 2_000_000);

        let json = serde_json::to_string(&original).unwrap();
        let round_tripped: FileMetadata = serde_json::from_str(&json).unwrap();
        assert_eq!(round_tripped, original);
        // BTreeMap iteration order is by key — deterministic across runs.
        let keys: Vec<_> = round_tripped.cache_cooldown_until_ms.keys().copied().collect();
        assert_eq!(keys, vec![0, 2]);
    }

    // ── Bug BC-10 — Quarantine state-machine apply ──────────────────

    fn mk_quarantinable_node() -> crate::NodeInfo {
        crate::NodeInfo {
            role: dtfs_common::types::NodeRole::Data,
            hostname: "data-1".into(),
            grpc_addr: "data-1:9500".into(),
            location: dtfs_common::topology::Location::new("lab", "dc-a", "r1", "h1"),
            disks: Vec::new(),
            total_capacity: 0,
            used_capacity: 0,
            health: dtfs_common::types::HealthStatus::Healthy,
            last_seen_ms: 0,
            total_bytes_read: 0,
            total_bytes_written: 0,
            client_ip: None,
            version: String::new(),
            build_date: String::new(),
            quarantine: None,
        }
    }

    fn mk_quarantine_info(reason: dtfs_common::types::QuarantineReason) -> dtfs_common::types::QuarantineInfo {
        dtfs_common::types::QuarantineInfo {
            since_ms: 1_000,
            reason,
            triggering_chunk_id: Some("aabbccdd".into()),
            fault_count_total: 0,
            last_fault_at_ms: 1_000,
            last_fault_summary: "bytes mismatched expected hash".into(),
        }
    }

    // ── Bug BJ — RegisterNode audit idempotency ────────────────────

    fn count_register_audits(sm: &StateMachineData) -> usize {
        sm.audit_log
            .iter()
            .filter(|e| e.summary.starts_with("RegisterNode:"))
            .count()
    }

    #[test]
    fn bug_bj_register_node_new_emits_audit() {
        let mut sm = StateMachineData::default();
        apply_request(
            &mut sm,
            &mk_log_id(1),
            &ClientRequest::RegisterNode { now_ms: 0, node_id: 7, info: mk_quarantinable_node() },
        );
        assert_eq!(count_register_audits(&sm), 1, "first register emits one row");
    }

    #[test]
    fn bug_bj_register_node_reregister_identical_no_audit() {
        let mut sm = StateMachineData::default();
        let info = mk_quarantinable_node();
        apply_request(
            &mut sm,
            &mk_log_id(1),
            &ClientRequest::RegisterNode { now_ms: 0, node_id: 7, info: info.clone() },
        );
        // Re-register: only volatile fields differ (heartbeat-driven).
        let mut info2 = info;
        info2.last_seen_ms = 999_999;
        info2.used_capacity = 12_345;
        info2.total_bytes_read = 555;
        info2.total_bytes_written = 666;
        apply_request(
            &mut sm,
            &mk_log_id(2),
            &ClientRequest::RegisterNode { now_ms: 0, node_id: 7, info: info2 },
        );
        assert_eq!(
            count_register_audits(&sm),
            1,
            "identical re-register (only volatile fields changed) must add no audit row"
        );
    }

    #[test]
    fn bug_bj_register_node_material_change_emits_audit() {
        let mut sm = StateMachineData::default();
        let info = mk_quarantinable_node();
        apply_request(
            &mut sm,
            &mk_log_id(1),
            &ClientRequest::RegisterNode { now_ms: 0, node_id: 7, info: info.clone() },
        );
        let mut info2 = info;
        info2.version = "v0.0.1-new".into(); // material identity field
        apply_request(
            &mut sm,
            &mk_log_id(2),
            &ClientRequest::RegisterNode { now_ms: 0, node_id: 7, info: info2 },
        );
        assert_eq!(
            count_register_audits(&sm),
            2,
            "a materially-changed re-register (version) must add exactly one row"
        );
    }

    // ── RAFT-1 — RegisterNode must carry forward sticky Quarantined/
    // Maintenance/Draining state instead of resetting to Healthy ─────

    #[test]
    fn raft_1_reregister_keeps_quarantined() {
        use dtfs_common::types::{HealthStatus, QuarantineReason};
        let mut sm = StateMachineData::default();
        apply_request(
            &mut sm,
            &mk_log_id(1),
            &ClientRequest::RegisterNode { now_ms: 0, node_id: 7, info: mk_quarantinable_node() },
        );
        apply_request(
            &mut sm,
            &mk_log_id(2),
            &ClientRequest::QuarantineNode { now_ms: 0,
                node_id: 7,
                info: mk_quarantine_info(QuarantineReason::DeepScrubHashMismatch),
            },
        );
        assert_eq!(sm.nodes.get(&7).unwrap().health, HealthStatus::Quarantined);

        // Re-register (e.g. process restart) with the producer's
        // unconditional health: Healthy, quarantine: None.
        apply_request(
            &mut sm,
            &mk_log_id(3),
            &ClientRequest::RegisterNode { now_ms: 0, node_id: 7, info: mk_quarantinable_node() },
        );
        let node = sm.nodes.get(&7).expect("node still registered");
        assert_eq!(node.health, HealthStatus::Quarantined,
            "re-register must not clear Quarantined");
        assert!(node.quarantine.is_some(),
            "QuarantineInfo must survive re-register");
    }

    /// Audit #7 — a refused UpdateNodeStatus on a Quarantined node must
    /// return an Error (A.3.6), not Success; down/failed still apply.
    #[test]
    fn update_node_status_on_quarantined_node_errors() {
        use dtfs_common::types::{HealthStatus, QuarantineReason};
        let mut sm = StateMachineData::default();
        apply_request(&mut sm, &mk_log_id(1),
            &ClientRequest::RegisterNode { now_ms: 0, node_id: 7, info: mk_quarantinable_node() });
        apply_request(&mut sm, &mk_log_id(2), &ClientRequest::QuarantineNode { now_ms: 0,
            node_id: 7, info: mk_quarantine_info(QuarantineReason::DeepScrubHashMismatch) });
        let r = apply_request(&mut sm, &mk_log_id(3),
            &ClientRequest::UpdateNodeStatus { now_ms: 0, node_id: 7, status: "maintenance".into() });
        assert!(matches!(&r, ClientResponse::Error { message } if message.contains("is quarantined")), "{r:?}");
        assert_eq!(sm.nodes.get(&7).unwrap().health, HealthStatus::Quarantined);
        let r = apply_request(&mut sm, &mk_log_id(4),
            &ClientRequest::UpdateNodeStatus { now_ms: 0, node_id: 7, status: "down".into() });
        assert!(matches!(r, ClientResponse::Success), "{r:?}");
        assert_eq!(sm.nodes.get(&7).unwrap().health, HealthStatus::Down);
    }

    #[test]
    fn raft_1_reregister_keeps_maintenance() {
        use dtfs_common::types::HealthStatus;
        let mut sm = StateMachineData::default();
        apply_request(
            &mut sm,
            &mk_log_id(1),
            &ClientRequest::RegisterNode { now_ms: 0, node_id: 7, info: mk_quarantinable_node() },
        );
        apply_request(
            &mut sm,
            &mk_log_id(2),
            &ClientRequest::UpdateNodeStatus { now_ms: 0, node_id: 7, status: "maintenance".into() },
        );
        assert_eq!(sm.nodes.get(&7).unwrap().health, HealthStatus::Maintenance);
        apply_request(
            &mut sm,
            &mk_log_id(3),
            &ClientRequest::RegisterNode { now_ms: 0, node_id: 7, info: mk_quarantinable_node() },
        );
        assert_eq!(sm.nodes.get(&7).unwrap().health, HealthStatus::Maintenance,
            "re-register must not clear Maintenance");
    }

    #[test]
    fn raft_1_reregister_healthy_stays_healthy() {
        use dtfs_common::types::HealthStatus;
        let mut sm = StateMachineData::default();
        apply_request(
            &mut sm,
            &mk_log_id(1),
            &ClientRequest::RegisterNode { now_ms: 0, node_id: 7, info: mk_quarantinable_node() },
        );
        apply_request(
            &mut sm,
            &mk_log_id(2),
            &ClientRequest::RegisterNode { now_ms: 0, node_id: 7, info: mk_quarantinable_node() },
        );
        assert_eq!(sm.nodes.get(&7).unwrap().health, HealthStatus::Healthy);
    }

    // Bug BR-1 — managers now self-register (role="manager") to publish
    // their version/build_date and a live last_seen_ms into shared state.
    // The same audit-idempotency that protects data-node re-registration
    // must hold for the manager keepalive: a steady-state beat (only
    // last_seen_ms changes) adds no audit row, while a real upgrade
    // (version change) logs exactly once. Guards the new manager keepalive
    // from spamming the audit feed.
    #[test]
    fn bug_br1_manager_self_register_versioned_and_idempotent() {
        let mut sm = StateMachineData::default();
        let mut info = mk_quarantinable_node();
        info.role = dtfs_common::types::NodeRole::Manager;
        info.hostname = "mgr01".into();
        info.grpc_addr = "10.0.0.1:9401".into();
        info.version = "v1.2.3".into();
        info.build_date = "20260602-000000".into();
        apply_request(
            &mut sm,
            &mk_log_id(1),
            &ClientRequest::RegisterNode { now_ms: 0, node_id: 42, info: info.clone() },
        );
        assert_eq!(
            sm.nodes.get(&42).map(|n| n.version.as_str()),
            Some("v1.2.3"),
            "manager self-register must store version in shared state"
        );
        assert_eq!(count_register_audits(&sm), 1, "first manager register emits one row");

        // Keepalive beat: only last_seen_ms moves.
        let mut beat = info.clone();
        beat.last_seen_ms = 123_456;
        apply_request(
            &mut sm,
            &mk_log_id(2),
            &ClientRequest::RegisterNode { now_ms: 0, node_id: 42, info: beat },
        );
        assert_eq!(
            count_register_audits(&sm),
            1,
            "manager keepalive re-register must not spam the audit log"
        );

        // Upgrade: version changes once.
        let mut upgraded = info;
        upgraded.version = "v1.2.4".into();
        apply_request(
            &mut sm,
            &mk_log_id(3),
            &ClientRequest::RegisterNode { now_ms: 0, node_id: 42, info: upgraded },
        );
        assert_eq!(
            count_register_audits(&sm),
            2,
            "a manager version change (upgrade) must log exactly one row"
        );
    }

    // Bug BY — applying a ReportScrubResult (now produced by the data node
    // via heartbeat) records the result per (node, level) so the WebUI
    // "Last Scrub" card + stats.scrub_results_by_node populate. A deep
    // result does not overwrite the quick one (BK).
    #[test]
    fn bug_by_report_scrub_result_populates_state() {
        let mut sm = StateMachineData::default();
        apply_request(
            &mut sm,
            &mk_log_id(1),
            &ClientRequest::ReportScrubResult { now_ms: 0,
                node_id: 5,
                disk_path: "/mnt/data01-warm".into(),
                level: "quick".into(),
                completed_at_ms: 1_700_000_000_000,
                chunks_checked: 160,
                chunks_healthy: 160,
                chunks_corrupt: 0,
                chunks_missing: 0,
                duration_secs: 0.05,
            },
        );
        let rec = sm
            .scrub_results_by_level
            .get(&5)
            .and_then(|m| m.get("quick"))
            .expect("quick record stored per (node, level)");
        assert_eq!(rec.chunks_checked, 160);
        assert_eq!(rec.level, "quick");
        assert!(sm.scrub_results.contains_key(&5), "legacy per-node map also populated");

        apply_request(
            &mut sm,
            &mk_log_id(2),
            &ClientRequest::ReportScrubResult { now_ms: 0,
                node_id: 5,
                disk_path: "/mnt/data01-warm".into(),
                level: "deep".into(),
                completed_at_ms: 1_700_000_001_000,
                chunks_checked: 314,
                chunks_healthy: 314,
                chunks_corrupt: 0,
                chunks_missing: 0,
                duration_secs: 1.2,
            },
        );
        assert_eq!(
            sm.scrub_results_by_level.get(&5).unwrap().len(),
            2,
            "quick + deep both retained (BK), not overwritten"
        );
    }

    fn mk_data_node_with_disk(used_bytes: u64) -> crate::NodeInfo {
        let mut n = mk_quarantinable_node();
        n.disks = vec![dtfs_common::types::DiskInfo {
            disk_id: 0,
            uuid: "uuid-0".into(),
            mount_path: "/mnt/d0".into(),
            tier: dtfs_common::types::DiskTier::Warm,
            total_bytes: 1_000_000,
            used_bytes,
            status: dtfs_common::types::DiskStatus::Healthy,
        }];
        n
    }

    #[test]
    fn bug_bj_register_node_disk_usage_change_no_audit() {
        // A data node reports live per-disk used_bytes on every register;
        // a drift there must NOT count as a material change.
        let mut sm = StateMachineData::default();
        apply_request(
            &mut sm,
            &mk_log_id(1),
            &ClientRequest::RegisterNode { now_ms: 0, node_id: 7, info: mk_data_node_with_disk(100) },
        );
        apply_request(
            &mut sm,
            &mk_log_id(2),
            &ClientRequest::RegisterNode { now_ms: 0, node_id: 7, info: mk_data_node_with_disk(999_999) },
        );
        assert_eq!(
            count_register_audits(&sm),
            1,
            "only disk used_bytes changed → no new audit row"
        );
    }

    #[test]
    fn bug_bj_register_node_disk_status_change_emits_audit() {
        // A disk status flip (e.g. Failed) IS material.
        let mut sm = StateMachineData::default();
        apply_request(
            &mut sm,
            &mk_log_id(1),
            &ClientRequest::RegisterNode { now_ms: 0, node_id: 7, info: mk_data_node_with_disk(100) },
        );
        let mut changed = mk_data_node_with_disk(100);
        changed.disks[0].status = dtfs_common::types::DiskStatus::Failed;
        apply_request(
            &mut sm,
            &mk_log_id(2),
            &ClientRequest::RegisterNode { now_ms: 0, node_id: 7, info: changed },
        );
        assert_eq!(
            count_register_audits(&sm),
            2,
            "a disk status change must add an audit row"
        );
    }

    #[test]
    fn bug_bj_register_node_disk_reorder_no_audit() {
        use dtfs_common::types::{DiskInfo, DiskStatus, DiskTier};
        let warm = DiskInfo {
            disk_id: 0,
            uuid: "u-warm".into(),
            mount_path: "/mnt/w".into(),
            tier: DiskTier::Warm,
            total_bytes: 100,
            used_bytes: 10,
            status: DiskStatus::Healthy,
        };
        let cold = DiskInfo {
            disk_id: 1,
            uuid: "u-cold".into(),
            mount_path: "/mnt/c".into(),
            tier: DiskTier::Cold,
            total_bytes: 100,
            used_bytes: 5,
            status: DiskStatus::Healthy,
        };
        let mut n1 = mk_quarantinable_node();
        n1.disks = vec![warm.clone(), cold.clone()];
        // Re-register: disks reversed, disk_id reassigned, used_bytes
        // drifted, AND uuids regenerated (the data node mints fresh disk
        // uuids per boot) — same physical mounts, so NOT material.
        let mut cold_b = cold;
        cold_b.disk_id = 0;
        cold_b.used_bytes = 99;
        cold_b.uuid = "u-cold-boot2".into();
        let mut warm_b = warm;
        warm_b.disk_id = 1;
        warm_b.used_bytes = 42;
        warm_b.uuid = "u-warm-boot2".into();
        let mut n2 = mk_quarantinable_node();
        n2.disks = vec![cold_b, warm_b];

        let mut sm = StateMachineData::default();
        apply_request(
            &mut sm,
            &mk_log_id(1),
            &ClientRequest::RegisterNode { now_ms: 0, node_id: 7, info: n1 },
        );
        apply_request(
            &mut sm,
            &mk_log_id(2),
            &ClientRequest::RegisterNode { now_ms: 0, node_id: 7, info: n2 },
        );
        assert_eq!(
            count_register_audits(&sm),
            1,
            "disk reorder + disk_id swap + usage drift → no new audit row"
        );
    }

    #[test]
    fn bug_bc_10_quarantine_node_sets_health_and_info() {
        use dtfs_common::types::{HealthStatus, QuarantineReason};
        let mut sm = StateMachineData::default();
        sm.nodes.insert(42, mk_quarantinable_node());

        let info = mk_quarantine_info(QuarantineReason::VerifyOnReadMismatch);
        let resp = apply_request(
            &mut sm,
            &mk_log_id(1),
            &ClientRequest::QuarantineNode { now_ms: 0, node_id: 42, info: info.clone() },
        );
        assert!(matches!(resp, ClientResponse::Success));

        let n = sm.nodes.get(&42).expect("node still present");
        assert_eq!(n.health, HealthStatus::Quarantined);
        let q = n.quarantine.as_ref().expect("quarantine record present");
        assert_eq!(q.reason, QuarantineReason::VerifyOnReadMismatch);
        assert_eq!(q.triggering_chunk_id.as_deref(), Some("aabbccdd"));
        assert_eq!(q.fault_count_total, 1, "first fault accumulates to 1");

        // Audit row should mention quarantine + reason.
        let last = sm.audit_log.back().unwrap();
        assert_eq!(last.summary, "node.quarantine: 42 reason=VerifyOnReadMismatch chunk=aabbccdd fault_count=1");

        // Bug BH-9 — the row must carry STRUCTURED fields so the
        // integrity feed renders TARGET (node id + hostname) distinctly
        // from RESULT/SUMMARY (the fault detail), not the same blob in
        // both columns.
        assert_eq!(last.actor, "system");
        assert_eq!(last.action, "node.quarantine");
        assert_eq!(last.target, "42 (data-1)", "TARGET = node id + hostname only");
        assert!(
            !last.target.contains("reason=") && !last.target.contains("chunk="),
            "TARGET must not carry fault detail: {}",
            last.target
        );
        assert!(
            last.result.contains("reason=VerifyOnReadMismatch")
                && last.result.contains("chunk=aabbccdd"),
            "RESULT must carry the fault detail: {}",
            last.result
        );
    }

    #[test]
    fn bug_bc_10_quarantine_accumulates_fault_count() {
        use dtfs_common::types::QuarantineReason;
        let mut sm = StateMachineData::default();
        sm.nodes.insert(7, mk_quarantinable_node());

        for i in 1..=3 {
            apply_request(
                &mut sm,
                &mk_log_id(i),
                &ClientRequest::QuarantineNode { now_ms: 0,
                    node_id: 7,
                    info: mk_quarantine_info(QuarantineReason::DeepScrubHashMismatch),
                },
            );
        }
        let q = sm.nodes.get(&7).unwrap().quarantine.as_ref().unwrap();
        assert_eq!(q.fault_count_total, 3,
            "three faults must accumulate, not reset on each apply");
    }

    #[test]
    fn bug_bc_10_unquarantine_clears_and_restores_health() {
        use dtfs_common::types::{HealthStatus, QuarantineReason};
        let mut sm = StateMachineData::default();
        sm.nodes.insert(9, mk_quarantinable_node());
        apply_request(
            &mut sm,
            &mk_log_id(1),
            &ClientRequest::QuarantineNode { now_ms: 0,
                node_id: 9,
                info: mk_quarantine_info(QuarantineReason::QuickScrubMissingDat),
            },
        );
        assert_eq!(sm.nodes.get(&9).unwrap().health, HealthStatus::Quarantined);

        let resp = apply_request(
            &mut sm,
            &mk_log_id(2),
            &ClientRequest::UnquarantineNode { now_ms: 0,
                node_id: 9,
                operator_id: "alice".into(),
                reason_acknowledged: "disk swapped + bitrot test passed".into(),
            },
        );
        assert!(matches!(resp, ClientResponse::Success));

        let n = sm.nodes.get(&9).unwrap();
        assert_eq!(n.health, HealthStatus::Healthy,
            "unquarantine must restore Healthy, not leave the node in Suspect");
        assert!(n.quarantine.is_none(),
            "quarantine record must be cleared");

        let last = sm.audit_log.back().unwrap();
        assert!(last.summary.contains("node.unquarantine: 9"),
            "audit must record the unquarantine event: {}", last.summary);
        assert!(last.summary.contains("by=alice"));
        assert!(last.summary.contains("ack=disk swapped"));
    }

    /// Bug BC-11 — once a node is Quarantined, the canonical
    /// `health == Healthy` filter used by `placement::rebuild_ring`,
    /// `scheduler::resolve_healthy_source`, and `api::allocate_chunks`
    /// MUST exclude it. We exercise the filter directly here so a
    /// future refactor that flips one of those sites to a negative
    /// `health != Down` predicate (which would leak Quarantined into
    /// the serving pool) is caught immediately.
    #[test]
    fn bug_bc_11_quarantined_node_excluded_from_healthy_filter() {
        use dtfs_common::types::{HealthStatus, QuarantineReason};
        let mut sm = StateMachineData::default();
        sm.nodes.insert(1, mk_quarantinable_node());
        sm.nodes.insert(2, mk_quarantinable_node());
        sm.nodes.insert(3, mk_quarantinable_node());

        // Quarantine node 2.
        apply_request(
            &mut sm,
            &mk_log_id(1),
            &ClientRequest::QuarantineNode { now_ms: 0,
                node_id: 2,
                info: mk_quarantine_info(QuarantineReason::VerifyOnReadMismatch),
            },
        );

        let healthy: Vec<u64> = sm
            .nodes
            .iter()
            .filter(|(_, n)| n.health == HealthStatus::Healthy)
            .map(|(id, _)| *id)
            .collect();
        let mut sorted = healthy.clone();
        sorted.sort();
        assert_eq!(sorted, vec![1, 3],
            "Quarantined node 2 must NOT appear in the healthy serving \
             pool. If this assertion ever fires after a placement or \
             scheduler refactor, the cluster is at risk of routing reads \
             or replication to a node we know is corrupt.");
    }

    #[test]
    fn bug_bc_10_quarantine_missing_node_returns_error() {
        use dtfs_common::types::QuarantineReason;
        let mut sm = StateMachineData::default();
        let resp = apply_request(
            &mut sm,
            &mk_log_id(1),
            &ClientRequest::QuarantineNode { now_ms: 0,
                node_id: 999,
                info: mk_quarantine_info(QuarantineReason::OrphanScrubInStateMissing),
            },
        );
        assert!(matches!(resp, ClientResponse::Error { .. }));
    }

    // ── Bug BK — scrub results per (node, level) ────────────────────

    fn mk_scrub_req(node_id: u64, level: &str, completed_at_ms: u64) -> ClientRequest {
        ClientRequest::ReportScrubResult { now_ms: 0,
            node_id,
            disk_path: "/mnt/d0".into(),
            level: level.into(),
            completed_at_ms,
            chunks_checked: 10,
            chunks_healthy: 10,
            chunks_corrupt: 0,
            chunks_missing: 0,
            duration_secs: 1.0,
        }
    }

    #[test]
    fn scrub_quick_then_deep_both_retained() {
        let mut sm = StateMachineData::default();
        apply_request(&mut sm, &mk_log_id(1), &mk_scrub_req(7, "quick", 1_000));
        apply_request(&mut sm, &mk_log_id(2), &mk_scrub_req(7, "deep", 2_000));
        let by_level = sm.scrub_results_by_level.get(&7).expect("node 7 present");
        assert!(by_level.contains_key("quick"), "quick result retained");
        assert!(by_level.contains_key("deep"), "deep result retained");
        assert_eq!(by_level.len(), 2, "both levels coexist (deep did not overwrite quick)");
        // Same level re-reports → overwrite (latest of each level).
        apply_request(&mut sm, &mk_log_id(3), &mk_scrub_req(7, "quick", 3_000));
        let by_level = sm.scrub_results_by_level.get(&7).unwrap();
        assert_eq!(by_level.len(), 2);
        assert_eq!(by_level.get("quick").unwrap().completed_at_ms, 3_000,
            "latest quick replaces the earlier quick");
    }

    #[test]
    fn scrub_snapshot_roundtrip_preserves_levels() {
        let mut sm = StateMachineData::default();
        apply_request(&mut sm, &mk_log_id(1), &mk_scrub_req(7, "quick", 1_000));
        apply_request(&mut sm, &mk_log_id(2), &mk_scrub_req(7, "deep", 2_000));
        // Round-trip the nested map through serde (snapshot uses JSON).
        let json = serde_json::to_string(&sm.scrub_results_by_level).unwrap();
        let restored: std::collections::BTreeMap<u64, std::collections::BTreeMap<String, ScrubRecord>> =
            serde_json::from_str(&json).unwrap();
        let by_level = restored.get(&7).expect("node 7 survives round-trip");
        assert!(by_level.contains_key("quick") && by_level.contains_key("deep"),
            "both levels survive snapshot serialize/deserialize");
    }

    #[test]
    fn scrub_legacy_snapshot_migrates() {
        // Simulate a pre-BK snapshot: only the legacy single-per-node map is
        // populated; the nested map is empty. migrate_* must seed it.
        let mut sm = StateMachineData::default();
        sm.scrub_results.insert(7, ScrubRecord {
            node_id: 7,
            disk_path: "/mnt/d0".into(),
            level: "deep".into(),
            completed_at_ms: 5_000,
            chunks_checked: 3,
            chunks_healthy: 3,
            chunks_corrupt: 0,
            chunks_missing: 0,
            duration_secs: 0.5,
        });
        assert!(sm.scrub_results_by_level.is_empty());
        migrate_scrub_results_by_level(&mut sm);
        let by_level = sm.scrub_results_by_level.get(&7).expect("migrated");
        assert!(by_level.contains_key("deep"),
            "legacy record migrates into nested map keyed by its own level");
        // Idempotent: a second call must not clobber freshly-applied entries.
        apply_request(&mut sm, &mk_log_id(1), &mk_scrub_req(7, "quick", 6_000));
        migrate_scrub_results_by_level(&mut sm);
        assert_eq!(sm.scrub_results_by_level.get(&7).unwrap().len(), 2,
            "migration is a no-op once the nested map is populated");
    }

    // ── Bug BN-2 — heartbeat audit spam suppression ─────────────────

    fn count_audit_prefix(sm: &StateMachineData, prefix: &str) -> usize {
        sm.audit_log.iter().filter(|e| e.summary.starts_with(prefix)).count()
    }

    #[test]
    fn bug_bn2_update_node_usage_emits_no_audit() {
        let mut sm = StateMachineData::default();
        apply_request(&mut sm, &mk_log_id(1),
            &ClientRequest::RegisterNode { now_ms: 0, node_id: 7, info: mk_quarantinable_node() });
        let before = sm.audit_log.len();
        apply_request(&mut sm, &mk_log_id(2), &ClientRequest::UpdateNodeUsage {
            node_id: 7, used_capacity: 123, chunk_count: 4,
            total_bytes_read: 5, total_bytes_written: 6, observed_at_ms: 1_000,
        });
        assert_eq!(sm.audit_log.len(), before,
            "UpdateNodeUsage is telemetry — it must add no audit row");
    }

    #[test]
    fn bug_bn2_update_node_status_no_audit_without_transition() {
        let mut sm = StateMachineData::default();
        apply_request(&mut sm, &mk_log_id(1),
            &ClientRequest::RegisterNode { now_ms: 0, node_id: 7, info: mk_quarantinable_node() });
        // mk_quarantinable_node() is Healthy; applying "healthy" twice is a
        // no-op transition each time.
        apply_request(&mut sm, &mk_log_id(2),
            &ClientRequest::UpdateNodeStatus { now_ms: 0, node_id: 7, status: "healthy".into() });
        apply_request(&mut sm, &mk_log_id(3),
            &ClientRequest::UpdateNodeStatus { now_ms: 0, node_id: 7, status: "healthy".into() });
        assert_eq!(count_audit_prefix(&sm, "UpdateNodeStatus:"), 0,
            "Healthy -> Healthy must not audit");
    }

    #[test]
    fn bug_bn2_update_node_status_audits_real_transition() {
        let mut sm = StateMachineData::default();
        apply_request(&mut sm, &mk_log_id(1),
            &ClientRequest::RegisterNode { now_ms: 0, node_id: 7, info: mk_quarantinable_node() });
        apply_request(&mut sm, &mk_log_id(2),
            &ClientRequest::UpdateNodeStatus { now_ms: 0, node_id: 7, status: "suspect".into() });
        apply_request(&mut sm, &mk_log_id(3),
            &ClientRequest::UpdateNodeStatus { now_ms: 0, node_id: 7, status: "healthy".into() });
        assert_eq!(count_audit_prefix(&sm, "UpdateNodeStatus:"), 2,
            "Healthy->Suspect then Suspect->Healthy = two transitions = two rows");
    }

    // ── RAFT-2 — RenameFile must move directory children, not just the
    // exact key ──────────────────────────────────────────────────────

    #[test]
    fn raft_2_rename_directory_moves_children() {
        let mut sm = StateMachineData::default();
        apply_request(&mut sm, &mk_log_id(1), &ClientRequest::CreateFile { now_ms: 0,
            path: "/a/".into(), volume_id: "default".into(), chunk_size: 4096,
        });
        apply_request(&mut sm, &mk_log_id(2), &ClientRequest::CreateFile { now_ms: 0,
            path: "/a/x".into(), volume_id: "default".into(), chunk_size: 4096,
        });
        apply_request(&mut sm, &mk_log_id(3), &ClientRequest::CreateFile { now_ms: 0,
            path: "/a/b/y".into(), volume_id: "default".into(), chunk_size: 4096,
        });
        apply_request(&mut sm, &mk_log_id(4),
            &ClientRequest::RenameFile { now_ms: 0, from: "/a".into(), to: "/c".into() });

        assert!(sm.files.contains_key("/c"), "dir marker moved");
        assert!(sm.files.contains_key("/c/x"), "child file moved");
        assert!(sm.files.contains_key("/c/b/y"), "grandchild moved");
        assert_eq!(sm.files.get("/c/x").unwrap().path, "/c/x",
            "moved child's own path field must be updated");
        assert!(!sm.files.contains_key("/a"), "old dir marker gone");
        assert!(!sm.files.contains_key("/a/x"), "old child gone");
        assert!(!sm.files.contains_key("/a/b/y"), "old grandchild gone");
    }

    #[test]
    fn raft_2_rename_synthetic_directory_moves_children() {
        let mut sm = StateMachineData::default();
        // No mkdir marker for "/a" — only a file underneath it, so "/a" is
        // a synthetic directory (Bug B).
        apply_request(&mut sm, &mk_log_id(1), &ClientRequest::CreateFile { now_ms: 0,
            path: "/a/x".into(), volume_id: "default".into(), chunk_size: 4096,
        });
        let resp = apply_request(&mut sm, &mk_log_id(2),
            &ClientRequest::RenameFile { now_ms: 0, from: "/a".into(), to: "/c".into() });
        assert!(matches!(resp, ClientResponse::Success),
            "synthetic-dir source with children must succeed, got {resp:?}");
        assert!(sm.files.contains_key("/c/x"));
        assert!(!sm.files.contains_key("/a/x"));
    }

    #[test]
    fn raft_2_rename_onto_nonempty_directory_is_refused() {
        let mut sm = StateMachineData::default();
        apply_request(&mut sm, &mk_log_id(1), &ClientRequest::CreateFile { now_ms: 0,
            path: "/a/x".into(), volume_id: "default".into(), chunk_size: 4096,
        });
        apply_request(&mut sm, &mk_log_id(2), &ClientRequest::CreateFile { now_ms: 0,
            path: "/b/y".into(), volume_id: "default".into(), chunk_size: 4096,
        });
        let resp = apply_request(&mut sm, &mk_log_id(3),
            &ClientRequest::RenameFile { now_ms: 0, from: "/a".into(), to: "/b".into() });
        assert!(matches!(resp, ClientResponse::Error { .. }),
            "renaming onto a non-empty directory must be refused, got {resp:?}");
        assert!(sm.files.contains_key("/a/x"), "source untouched on refusal");
        assert!(sm.files.contains_key("/b/y"), "destination untouched on refusal");
    }

    #[test]
    fn raft_2_rename_lease_follows_child() {
        let mut sm = StateMachineData::default();
        apply_request(&mut sm, &mk_log_id(1), &ClientRequest::CreateFile { now_ms: 0,
            path: "/a/x".into(), volume_id: "default".into(), chunk_size: 4096,
        });
        apply_request(&mut sm, &mk_log_id(2), &ClientRequest::AcquireWriteLease {
            path: "/a/x".into(), owner_id: "writer-1".into(), lease_ms: 60_000, now_ms: 1_000,
            proposed_lease_id: "lease-1".into(),
        });
        assert!(sm.write_leases.contains_key("/a/x"));
        apply_request(&mut sm, &mk_log_id(3),
            &ClientRequest::RenameFile { now_ms: 0, from: "/a".into(), to: "/c".into() });
        assert!(!sm.write_leases.contains_key("/a/x"), "old lease key gone");
        let lease = sm.write_leases.get("/c/x").expect("lease follows the renamed child");
        assert_eq!(lease.owner_id, "writer-1");
    }

    // ── RAFT-3 — logical_size must grow to cover chunks committed after a
    // truncate, not stay pinned at the truncated size forever ──────────

    #[test]
    fn raft_3_truncate_zero_then_commit_grows_logical_size() {
        let mut sm = StateMachineData::default();
        apply_request(&mut sm, &mk_log_id(1), &ClientRequest::CreateFile { now_ms: 0,
            path: "/f".into(), volume_id: "default".into(), chunk_size: 4 * 1024 * 1024,
        });
        apply_request(&mut sm, &mk_log_id(2),
            &ClientRequest::SetFileSize { now_ms: 0, path: "/f".into(), new_size: 0 });
        assert_eq!(sm.files.get("/f").unwrap().logical_size, Some(0));
        seed_lease(&mut sm, "/f");
        apply_request(&mut sm, &mk_log_id(3), &ClientRequest::UpdateChunkLocations { now_ms: 0,
            path: "/f".into(),
            chunks: vec![ChunkLocationEntry {
                chunk_index: 0, chunk_hash: "h".into(), node_id: "10".into(),
                disk_id: 0, size_bytes: 4 * 1024 * 1024,
            }],
            lease_id: "seed".into(),
        });
        assert_eq!(
            sm.files.get("/f").unwrap().logical_size,
            Some(4 * 1024 * 1024),
            "committing a chunk past the truncated size must grow logical_size"
        );
    }

    #[test]
    fn raft_3_sparse_grow_size_is_preserved_over_a_smaller_chunk_extent() {
        let mut sm = StateMachineData::default();
        apply_request(&mut sm, &mk_log_id(1), &ClientRequest::CreateFile { now_ms: 0,
            path: "/f".into(), volume_id: "default".into(), chunk_size: 4 * 1024 * 1024,
        });
        let six_mb = 6 * 1024 * 1024u64;
        apply_request(&mut sm, &mk_log_id(2),
            &ClientRequest::SetFileSize { now_ms: 0, path: "/f".into(), new_size: six_mb });
        apply_request(&mut sm, &mk_log_id(3), &ClientRequest::UpdateChunkLocations { now_ms: 0,
            path: "/f".into(),
            chunks: vec![ChunkLocationEntry {
                chunk_index: 0, chunk_hash: "h".into(), node_id: "10".into(),
                disk_id: 0, size_bytes: 4 * 1024 * 1024,
            }],
            lease_id: String::new(),
        });
        assert_eq!(
            sm.files.get("/f").unwrap().logical_size,
            Some(six_mb),
            "a sparse-grow size ahead of the committed chunks must not shrink"
        );
    }

    // ── RAFT-7 — DeleteFile must drop lease/locks and refuse a non-empty
    // directory ──────────────────────────────────────────────────────

    #[test]
    fn raft_7_delete_file_removes_lease_and_locks() {
        let mut sm = StateMachineData::default();
        apply_request(&mut sm, &mk_log_id(1), &ClientRequest::CreateFile { now_ms: 0,
            path: "/f".into(), volume_id: "default".into(), chunk_size: 4096,
        });
        apply_request(&mut sm, &mk_log_id(2), &ClientRequest::AcquireWriteLease {
            path: "/f".into(), owner_id: "writer-1".into(), lease_ms: 60_000, now_ms: 1_000,
            proposed_lease_id: "lease-1".into(),
        });
        assert!(sm.write_leases.contains_key("/f"));
        let resp = apply_request(&mut sm, &mk_log_id(3),
            &ClientRequest::DeleteFile { now_ms: 0, path: "/f".into() });
        assert!(matches!(resp, ClientResponse::Success));
        assert!(!sm.files.contains_key("/f"));
        assert!(!sm.write_leases.contains_key("/f"),
            "lease must be dropped, or a re-create within lease_ms hits a stale WriteLeaseConflict");
    }

    #[test]
    fn raft_7_delete_nonempty_directory_is_refused() {
        let mut sm = StateMachineData::default();
        apply_request(&mut sm, &mk_log_id(1), &ClientRequest::CreateFile { now_ms: 0,
            path: "/a/".into(), volume_id: "default".into(), chunk_size: 4096,
        });
        apply_request(&mut sm, &mk_log_id(2), &ClientRequest::CreateFile { now_ms: 0,
            path: "/a/x".into(), volume_id: "default".into(), chunk_size: 4096,
        });
        let resp = apply_request(&mut sm, &mk_log_id(3),
            &ClientRequest::DeleteFile { now_ms: 0, path: "/a".into() });
        assert!(matches!(resp, ClientResponse::Error { .. }),
            "rmdir of a non-empty directory must be refused, got {resp:?}");
        assert!(sm.files.contains_key("/a"), "dir survives refusal");
        assert!(sm.files.contains_key("/a/x"), "child survives refusal");
    }

    // ── RAFT-8 — UpdateConfig must always derive its own version, never
    // trust the wire value ──────────────────────────────────────────

    #[test]
    fn raft_8_update_config_version_always_current_plus_one() {
        let mut sm = StateMachineData::default();
        sm.cluster_config = ClusterConfig { config_json: "{}".into(), version: 5 };
        apply_request(&mut sm, &mk_log_id(1), &ClientRequest::UpdateConfig { now_ms: 0,
            config_json: "{\"a\":1}".into(),
            version: 1, // stale/regressive wire value
            expected_version: 5,
        });
        assert_eq!(sm.cluster_config.version, 6,
            "apply must derive version = current + 1, ignoring the wire value");
    }

    // ── RAFT-4 — apply of the seven now_ms-carrying variants must be a
    // pure function of (req, state), not each replica's own wall clock ──

    #[test]
    fn raft_4_rename_file_apply_is_deterministic_across_replicas() {
        let req = ClientRequest::RenameFile {
            from: "/a".into(), to: "/b".into(), now_ms: 1_234,
        };
        let mut sm1 = StateMachineData::default();
        apply_request(&mut sm1, &mk_log_id(1), &ClientRequest::CreateFile { now_ms: 0,
            path: "/a".into(), volume_id: "default".into(), chunk_size: 4096,
        });
        let mut sm2 = sm1.clone();
        apply_request(&mut sm1, &mk_log_id(2), &req);
        apply_request(&mut sm2, &mk_log_id(2), &req);
        assert_eq!(
            serde_json::to_vec(&sm1).unwrap(),
            serde_json::to_vec(&sm2).unwrap(),
            "two replicas applying the same now_ms-stamped entry must be byte-identical"
        );
        assert_eq!(sm1.files.get("/b").unwrap().updated_at_ms, 1_234);
    }

    #[test]
    fn raft_4_legacy_rename_file_without_now_ms_falls_back_to_log_index() {
        let legacy = r#"{"RenameFile":{"from":"/a","to":"/b"}}"#;
        let req: ClientRequest =
            serde_json::from_str(legacy).expect("legacy RenameFile (no now_ms) must deserialise");
        match &req {
            ClientRequest::RenameFile { now_ms, .. } => assert_eq!(*now_ms, 0),
            other => panic!("expected RenameFile, got {other:?}"),
        }
        let mut sm = StateMachineData::default();
        apply_request(&mut sm, &mk_log_id(1), &ClientRequest::CreateFile { now_ms: 0,
            path: "/a".into(), volume_id: "default".into(), chunk_size: 4096,
        });
        apply_request(&mut sm, &mk_log_id(7), &req);
        assert_eq!(
            sm.files.get("/b").unwrap().updated_at_ms, 7,
            "now_ms == 0 (legacy entry) must fall back to log_id.index"
        );
    }

    #[test]
    fn raft_4_update_chunk_locations_apply_is_deterministic_across_replicas() {
        let req = ClientRequest::UpdateChunkLocations {
            path: "/f".into(),
            chunks: vec![ChunkLocationEntry {
                chunk_index: 0, chunk_hash: "h".into(), node_id: "10".into(),
                disk_id: 0, size_bytes: 4096,
            }],
            lease_id: String::new(),
            now_ms: 5_000,
        };
        let mut sm1 = StateMachineData::default();
        apply_request(&mut sm1, &mk_log_id(1), &ClientRequest::CreateFile { now_ms: 0,
            path: "/f".into(), volume_id: "default".into(), chunk_size: 4096,
        });
        let mut sm2 = sm1.clone();
        apply_request(&mut sm1, &mk_log_id(2), &req);
        apply_request(&mut sm2, &mk_log_id(2), &req);
        assert_eq!(serde_json::to_vec(&sm1).unwrap(), serde_json::to_vec(&sm2).unwrap());
        assert_eq!(sm1.files.get("/f").unwrap().updated_at_ms, 5_000);
    }

    // ── RAFT-9 — a non-finite duration_secs must never reach the SM, or
    // the next snapshot fails to deserialize and gets discarded ────────

    #[test]
    fn raft_9_non_finite_duration_is_clamped_to_zero() {
        let mut sm = StateMachineData::default();
        apply_request(&mut sm, &mk_log_id(1), &ClientRequest::ReportScrubResult { now_ms: 0,
            node_id: 7,
            disk_path: "/data".into(),
            level: "quick".into(),
            completed_at_ms: 1_000,
            chunks_checked: 10,
            chunks_healthy: 10,
            chunks_corrupt: 0,
            chunks_missing: 0,
            duration_secs: f64::NAN,
        });
        let rec = sm.scrub_results.get(&7).expect("scrub result stored");
        assert_eq!(rec.duration_secs, 0.0, "NaN must be clamped to 0.0");
        // Round-trip through the same JSON encoding a snapshot would use.
        let json = serde_json::to_vec(&sm).expect("serialize");
        let restored: StateMachineData =
            serde_json::from_slice(&json).expect("must deserialize without a non-finite f64");
        assert_eq!(restored.scrub_results.get(&7).unwrap().duration_secs, 0.0);
    }
}

#[cfg(test)]
mod audit_co_tests {
    use super::*;
    use openraft::storage::{RaftSnapshotBuilder, RaftStateMachine};

    fn mk_log_id(idx: u64) -> LogId {
        use openraft::vote::RaftLeaderId;
        let leader = openraft::impls::leader_id_adv::LeaderId::<u64, u64>::new(0, 0);
        openraft::LogId::new(leader, idx)
    }

    // ----- CO-1-1 ----------------------------------------------------------
    // RemoveChunkLocation now carries a `disk_id`. A chunk may legally hold a
    // mandatory replica AND a cache replica on the SAME node_id across
    // DIFFERENT disks/tiers (CLAUDE.md A.3.4 / Bug BH); pre-fix the retain
    // keyed on (chunk_index, node_id) only and dropped BOTH (and the
    // production `debug_assert "drop exactly one entry"` panicked first).
    // Post-fix removal targets the exact (node, disk) and the co-located
    // replica survives.
    fn sm_with_colocated_disks() -> StateMachineData {
        // One file, one chunk_index, two replicas on the SAME node_id (10)
        // but different disk_id (0 cold anchor, 1 warm cache).
        let mut sm = StateMachineData::default();
        sm.files.insert("/x".to_string(), FileMetadata {
            path: "/x".to_string(),
            volume_id: "default".to_string(),
            chunk_size: 1024,
            chunks: vec![
                ChunkLocationEntry {
                    chunk_index: 0,
                    chunk_hash: "h".to_string(),
                    node_id: "10".to_string(),
                    disk_id: 0,
                    size_bytes: 1024,
                },
                ChunkLocationEntry {
                    chunk_index: 0,
                    chunk_hash: "h".to_string(),
                    node_id: "10".to_string(),
                    disk_id: 1,
                    size_bytes: 1024,
                },
            ],
            created_at_ms: 1,
            updated_at_ms: 1,
            symlink_target: None,
            logical_size: None,
            is_directory: false,
            cache_cooldown_until_ms: std::collections::BTreeMap::new(), ever_committed: None,
        });
        sm
    }

    #[test]
    fn co_1_1_remove_chunk_location_overbroad_drops_colocated_entry() {
        // Drop the WARM disk-1 replica; the cold disk-0 anchor must survive.
        let mut sm = sm_with_colocated_disks();
        let resp = apply_request(&mut sm, &mk_log_id(1),
            &ClientRequest::RemoveChunkLocation { now_ms: 0,
                path: "/x".into(),
                chunk_index: 0,
                node_id: "10".into(),
                disk_id: 1,
                enforce_anchor: None,
            });
        assert!(matches!(resp, ClientResponse::Success));
        // Only the targeted (node 10, disk 1) entry is removed; the
        // co-located same-node disk-0 cold anchor survives.
        let chunks = &sm.files.get("/x").unwrap().chunks;
        assert_eq!(
            chunks.len(),
            1,
            "RemoveChunkLocation must drop only the targeted (node,disk) entry, \
             leaving the co-located same-node different-disk replica intact",
        );
        assert_eq!(chunks[0].disk_id, 0, "the disk-0 cold anchor must be the survivor");
    }

    #[test]
    fn co_1_1_remove_chunk_location_legacy_sentinel_is_node_wide() {
        // The legacy sentinel (u32::MAX, what an old snapshot/log entry
        // deserializes to) keeps the pre-CO-1-1 node-wide behaviour: both
        // co-located entries on node 10 are dropped.
        let mut sm = sm_with_colocated_disks();
        let resp = apply_request(&mut sm, &mk_log_id(1),
            &ClientRequest::RemoveChunkLocation { now_ms: 0,
                path: "/x".into(),
                chunk_index: 0,
                node_id: "10".into(),
                disk_id: crate::legacy_disk_id(),
                enforce_anchor: None,
            });
        assert!(matches!(resp, ClientResponse::Success));
        assert_eq!(
            sm.files.get("/x").unwrap().chunks.len(),
            0,
            "legacy node-wide removal drops every (chunk_index, node) entry",
        );
    }

    #[test]
    fn co_1_1_remove_chunk_location_disk_id_defaults_to_legacy_sentinel() {
        // A.3.1 snapshot/log compat — a RemoveChunkLocation deserialized from
        // a pre-CO-1-1 log entry (no disk_id field) gets legacy_disk_id()
        // = u32::MAX and therefore applies node-wide.
        let json = r#"{"RemoveChunkLocation":{"path":"/x","chunk_index":0,"node_id":"10","enforce_anchor":null}}"#;
        let req: ClientRequest = serde_json::from_str(json).expect("legacy RemoveChunkLocation decodes");
        match &req {
            ClientRequest::RemoveChunkLocation { disk_id, .. } => {
                assert_eq!(*disk_id, crate::legacy_disk_id());
                assert_eq!(*disk_id, u32::MAX);
            }
            other => panic!("expected RemoveChunkLocation, got {other:?}"),
        }
        let mut sm = sm_with_colocated_disks();
        let resp = apply_request(&mut sm, &mk_log_id(1), &req);
        assert!(matches!(resp, ClientResponse::Success));
        assert_eq!(
            sm.files.get("/x").unwrap().chunks.len(),
            0,
            "a legacy (no disk_id) RemoveChunkLocation must apply node-wide",
        );
    }

    // ----- CO-3-1 ----------------------------------------------------------
    // apply_request stamps SystemTime::now() into replicated state
    // (FileMetadata.created_at_ms/updated_at_ms + AuditEntry.timestamp_ms)
    // instead of a now_ms carried in the command. Two replicas applying the
    // SAME CreateFile therefore diverge byte-for-byte. Correct: identical.
    #[test]
    fn co_3_1_create_file_apply_is_deterministic_across_replicas() {
        let req = ClientRequest::CreateFile { now_ms: 0,
            path: "/det.bin".into(),
            volume_id: "default".into(),
            chunk_size: 1024,
        };
        let mut a = StateMachineData::default();
        apply_request(&mut a, &mk_log_id(1), &req);

        // A real wall-clock gap between the two replays.
        std::thread::sleep(std::time::Duration::from_millis(5));

        let mut b = StateMachineData::default();
        apply_request(&mut b, &mk_log_id(1), &req);

        let a_bytes = serde_json::to_vec(&a).expect("serialize a");
        let b_bytes = serde_json::to_vec(&b).expect("serialize b");
        assert_eq!(
            a_bytes, b_bytes,
            "two replicas applying the SAME CreateFile must reach byte-identical \
             state; today now() is stamped at apply time so timestamps diverge",
        );
    }

    // ----- CO-4-9 ----------------------------------------------------------
    // CreateFile on an existing path inserts a fresh FileMetadata with empty
    // chunks, wiping the chunk list. Correct: an existing file's chunks are
    // preserved (CreateFile of an existing path must not silently truncate).
    #[test]
    fn co_4_9_create_file_existing_path_preserves_chunks() {
        let mut sm = StateMachineData::default();
        apply_request(&mut sm, &mk_log_id(1), &ClientRequest::CreateFile { now_ms: 0,
            path: "/keep.bin".into(),
            volume_id: "default".into(),
            chunk_size: 1024,
        });
        seed_lease(&mut sm, "/keep.bin");
        apply_request(&mut sm, &mk_log_id(2), &ClientRequest::UpdateChunkLocations { now_ms: 0,
            path: "/keep.bin".into(),
            chunks: vec![ChunkLocationEntry {
                chunk_index: 0,
                chunk_hash: "h".into(),
                node_id: "10".into(),
                disk_id: 0,
                size_bytes: 1024,
            }],
            lease_id: "seed".into(),
        });
        assert_eq!(sm.files.get("/keep.bin").unwrap().chunks.len(), 1);

        // Re-create the same path.
        apply_request(&mut sm, &mk_log_id(3), &ClientRequest::CreateFile { now_ms: 0,
            path: "/keep.bin".into(),
            volume_id: "default".into(),
            chunk_size: 1024,
        });
        assert_eq!(
            sm.files.get("/keep.bin").unwrap().chunks.len(),
            1,
            "CreateFile on an existing path must not wipe the chunk list",
        );
    }

    // ----- CO-4-10 ---------------------------------------------------------
    // RenameFile moves posix_locks but NOT write_leases, so a commit on the
    // new path under the still-valid lease is rejected with WriteLeaseConflict.
    // Correct: the lease follows the rename and the commit succeeds.
    #[test]
    fn co_4_10_rename_file_migrates_write_lease() {
        let mut sm = StateMachineData::default();
        // Grant a lease on /a.
        let granted = apply_request(&mut sm, &mk_log_id(1),
            &ClientRequest::AcquireWriteLease {
                path: "/a".into(),
                owner_id: "host:1:7".into(),
                lease_ms: 60_000,
                now_ms: 1_000,
                proposed_lease_id: "lease-xyz".into(),
            });
        let lease_id = match granted {
            ClientResponse::WriteLeaseGranted { lease_id, .. } => lease_id,
            other => panic!("expected WriteLeaseGranted, got {other:?}"),
        };
        // Create the file so UpdateChunkLocations has a target after rename.
        apply_request(&mut sm, &mk_log_id(2), &ClientRequest::CreateFile { now_ms: 0,
            path: "/a".into(),
            volume_id: "default".into(),
            chunk_size: 1024,
        });
        // Rename /a -> /b.
        apply_request(&mut sm, &mk_log_id(3), &ClientRequest::RenameFile { now_ms: 0,
            from: "/a".into(),
            to: "/b".into(),
        });
        // Commit chunks on /b under the original lease.
        let resp = apply_request(&mut sm, &mk_log_id(4),
            &ClientRequest::UpdateChunkLocations { now_ms: 0,
                path: "/b".into(),
                chunks: vec![ChunkLocationEntry {
                    chunk_index: 0,
                    chunk_hash: "h".into(),
                    node_id: "10".into(),
                    disk_id: 0,
                    size_bytes: 1024,
                }],
                lease_id,
            });
        assert!(
            matches!(resp, ClientResponse::Success),
            "the write lease must follow RenameFile so the holder can keep \
             committing under it; got {resp:?}",
        );
    }

    // ----- CO-4-11 ---------------------------------------------------------
    // pending_scrub is not part of StateMachineSnapshot, so build_snapshot
    // drops it and install_snapshot leaves the follower's stale value. Correct:
    // an installed snapshot makes the follower's pending_scrub match the source.
    #[tokio::test]
    async fn co_4_11_pending_scrub_survives_snapshot_install() {
        let src_dir = tempfile::tempdir().expect("tmp src");
        let snapshot_bytes;
        let snapshot_meta;
        let source_pending;
        {
            let mut src = StateMachineStore::open_at(src_dir.path()).expect("open src");
            {
                let mut data = src.data.write().await;
                apply_request(&mut data, &mk_log_id(1), &ClientRequest::TriggerScrub {
                    id: "src-scrub".into(),
                    level: "deep".into(),
                    requested_at_ms: 9_999,
                });
                source_pending = data.pending_scrub.clone();
            }
            let mut builder = src.get_snapshot_builder().await;
            let snap = builder.build_snapshot().await.expect("build");
            snapshot_meta = snap.meta.clone();
            snapshot_bytes = snap.snapshot.into_inner();
        }
        assert!(source_pending.is_some(), "precondition: source has a pending scrub");

        // Follower pre-seeded with a DIFFERENT pending_scrub.
        let fol_dir = tempfile::tempdir().expect("tmp follower");
        let mut follower = StateMachineStore::open_at(fol_dir.path()).expect("open follower");
        {
            let mut data = follower.data.write().await;
            apply_request(&mut data, &mk_log_id(1), &ClientRequest::TriggerScrub {
                id: "stale-follower".into(),
                level: "quick".into(),
                requested_at_ms: 1,
            });
        }
        follower
            .install_snapshot(&snapshot_meta, Cursor::new(snapshot_bytes))
            .await
            .expect("install");

        let data = follower.data.read().await;
        assert_eq!(
            data.pending_scrub, source_pending,
            "installing the source's snapshot must replace the follower's \
             pending_scrub with the source's value; today the field is not \
             part of StateMachineSnapshot so the stale follower value survives",
        );
    }

    // ----- CO-4-15 ---------------------------------------------------------
    // An empty lease_id bypasses the write-lease check on UpdateChunkLocations,
    // so a writer with no token can commit while holder A's lease is live.
    // Correct: with a live lease held by A, a lease-less commit is rejected.
    #[test]
    fn co_4_15_empty_lease_id_does_not_bypass_held_lease() {
        let mut sm = StateMachineData::default();
        // Holder A acquires the lease on /f.
        apply_request(&mut sm, &mk_log_id(1), &ClientRequest::AcquireWriteLease {
            path: "/f".into(),
            owner_id: "hostA:1:7".into(),
            lease_ms: 60_000,
            now_ms: 1_000,
            proposed_lease_id: "lease-A".into(),
        });
        apply_request(&mut sm, &mk_log_id(2), &ClientRequest::CreateFile { now_ms: 0,
            path: "/f".into(),
            volume_id: "default".into(),
            chunk_size: 1024,
        });
        // A different writer commits with an EMPTY lease_id while A holds it.
        let resp = apply_request(&mut sm, &mk_log_id(3),
            &ClientRequest::UpdateChunkLocations { now_ms: 0,
                path: "/f".into(),
                chunks: vec![ChunkLocationEntry {
                    chunk_index: 0,
                    chunk_hash: "h".into(),
                    node_id: "10".into(),
                    disk_id: 0,
                    size_bytes: 1024,
                }],
                lease_id: String::new(),
            });
        assert!(
            matches!(resp, ClientResponse::WriteLeaseConflict { .. }),
            "a commit with an empty lease_id must NOT bypass a live lease held \
             by another writer; got {resp:?}",
        );
    }

    // ----- CO-5-1 ----------------------------------------------------------
    // AcquireWriteLease computes `now_ms + lease_ms` unchecked, so lease_ms =
    // u64::MAX panics (debug overflow). Correct: no panic; grant a finite,
    // future expiry (saturating).
    #[test]
    fn co_5_1_acquire_write_lease_huge_lease_ms_does_not_overflow() {
        let mut sm = StateMachineData::default();
        let resp = apply_request(&mut sm, &mk_log_id(1),
            &ClientRequest::AcquireWriteLease {
                path: "/big".into(),
                owner_id: "host:1:7".into(),
                lease_ms: u64::MAX,
                now_ms: 1_000,
                proposed_lease_id: "lease-big".into(),
            });
        match resp {
            ClientResponse::WriteLeaseGranted { expires_at_ms, .. } => {
                assert!(
                    expires_at_ms > 1_000,
                    "expiry must be a finite future instant, got {expires_at_ms}",
                );
            }
            other => panic!("expected WriteLeaseGranted, got {other:?}"),
        }
    }

    // ----- CO-5-10 ---------------------------------------------------------
    // update_config read-then-writes `version = current + 1` and the apply
    // used to blind-overwrite. Two saves racing off the same base version
    // both target the same new version; without a CAS the second silently
    // loses the first's update. With `expected_version`, the apply rejects
    // the loser instead of clobbering the winner.
    #[test]
    fn co_5_10_update_config_rejects_stale_expected_version() {
        let mut sm = StateMachineData::default();
        assert_eq!(sm.cluster_config.version, 0);

        // Both writers read base version 0 and build version 1.
        let first = ClientRequest::UpdateConfig { now_ms: 0,
            config_json: r#"{"a":1}"#.into(),
            version: 1,
            expected_version: 0,
        };
        let second = ClientRequest::UpdateConfig { now_ms: 0,
            config_json: r#"{"b":2}"#.into(),
            version: 1,
            expected_version: 0,
        };

        let r1 = apply_request(&mut sm, &mk_log_id(1), &first);
        assert!(matches!(r1, ClientResponse::Success));
        assert_eq!(sm.cluster_config.version, 1);
        assert_eq!(sm.cluster_config.config_json, r#"{"a":1}"#);

        // The loser carries expected_version 0, which no longer matches the
        // live version 1 -> rejected, no overwrite.
        let r2 = apply_request(&mut sm, &mk_log_id(2), &second);
        assert!(
            matches!(r2, ClientResponse::Error { .. }),
            "stale-version UpdateConfig must be rejected, got {r2:?}",
        );
        assert_eq!(sm.cluster_config.version, 1, "loser must not bump version");
        assert_eq!(
            sm.cluster_config.config_json, r#"{"a":1}"#,
            "loser must not clobber the winner's config",
        );
    }

    // CO-5-10 compat (A.3.1): a UpdateConfig serialised before the
    // `expected_version` field existed must still deserialise (defaulting to
    // the UPDATE_CONFIG_NO_CAS sentinel) and apply WITHOUT a CAS check, so
    // replaying an old Raft log / snapshot never wedges on a phantom version
    // conflict.
    #[test]
    fn co_5_10_update_config_legacy_no_expected_version_applies() {
        let legacy = r#"{"UpdateConfig":{"config_json":"{}","version":7}}"#;
        let req: ClientRequest =
            serde_json::from_str(legacy).expect("legacy UpdateConfig must deserialise");
        match &req {
            ClientRequest::UpdateConfig { now_ms: 0, expected_version, version, .. } => {
                assert_eq!(
                    *expected_version,
                    crate::UPDATE_CONFIG_NO_CAS,
                    "missing field defaults to the no-CAS sentinel",
                );
                assert_eq!(*version, 7);
            }
            other => panic!("expected UpdateConfig, got {other:?}"),
        }

        // Apply against a non-zero live version; legacy entry (sentinel)
        // must skip the CAS and apply.
        let mut sm = StateMachineData::default();
        sm.cluster_config.version = 4;
        let resp = apply_request(&mut sm, &mk_log_id(1), &req);
        assert!(
            matches!(resp, ClientResponse::Success),
            "legacy entry must apply without a CAS check, got {resp:?}",
        );
        // RAFT-8 — the apply now ALWAYS derives version = current + 1 and
        // ignores the wire value (previously stored verbatim), closing a
        // version-regression hole a buggy/malicious proposer could hit even
        // when the CAS base matched. The legacy wire value of 7 here is
        // deliberately stale/irrelevant post-fix.
        assert_eq!(sm.cluster_config.version, 5);
    }

    // CO-5-10 — the version-0 boundary: two writes racing off a fresh
    // cluster both read base version 0 and carry expected_version 0. With a
    // sentinel-based skip (not a 0-based skip), the second is still CAS-
    // checked against the now-live version 1 and rejected, so the first
    // write is not lost on a brand-new cluster.
    #[test]
    fn co_5_10_concurrent_first_write_from_version_zero_is_cas_checked() {
        let mut sm = StateMachineData::default();
        assert_eq!(sm.cluster_config.version, 0);

        let first = ClientRequest::UpdateConfig { now_ms: 0,
            config_json: r#"{"a":1}"#.into(),
            version: 1,
            expected_version: 0,
        };
        let second = ClientRequest::UpdateConfig { now_ms: 0,
            config_json: r#"{"b":2}"#.into(),
            version: 1,
            expected_version: 0,
        };

        let r1 = apply_request(&mut sm, &mk_log_id(1), &first);
        assert!(matches!(r1, ClientResponse::Success));
        assert_eq!(sm.cluster_config.version, 1);

        let r2 = apply_request(&mut sm, &mk_log_id(2), &second);
        assert!(
            matches!(r2, ClientResponse::Error { .. }),
            "a second version-0-based write must be rejected once version is 1, got {r2:?}",
        );
        assert_eq!(sm.cluster_config.config_json, r#"{"a":1}"#);
    }
}

/// Raft audit fixes (symlink/link overwrite, rename type conflicts, ...).
#[cfg(test)]
mod raft_audit_fix_tests {
    use super::*;

    fn lid(idx: u64) -> LogId {
        use openraft::vote::RaftLeaderId;
        let leader = openraft::impls::leader_id_adv::LeaderId::<u64, u64>::new(0, 0);
        openraft::LogId::new(leader, idx)
    }

    fn create(sm: &mut StateMachineData, idx: u64, path: &str) {
        let r = apply_request(sm, &lid(idx), &ClientRequest::CreateFile { now_ms: 0,
            path: path.into(), volume_id: "default".into(), chunk_size: 4096,
        });
        assert!(matches!(r, ClientResponse::FileCreated { .. }), "{r:?}");
    }

    fn err_msg(r: ClientResponse) -> String {
        match r {
            ClientResponse::Error { message } => message,
            other => panic!("expected Error, got {other:?}"),
        }
    }

    #[test]
    fn symlink_refuses_existing_file_and_directory() {
        let mut sm = StateMachineData::default();
        create(&mut sm, 1, "/f");
        create(&mut sm, 2, "/d/x");
        let sym = |sm: &mut StateMachineData, i, p: &str| apply_request(sm, &lid(i),
            &ClientRequest::CreateSymlink { path: p.into(), target: "/t".into(), now_ms: 0 });
        assert_eq!(err_msg(sym(&mut sm, 3, "/f")), "Symlink: '/f' already exists");
        assert_eq!(err_msg(sym(&mut sm, 4, "/d")), "Symlink: '/d' already exists");
        assert!(sm.files["/f"].symlink_target.is_none(), "existing file untouched");
        // Sibling with a shared name prefix is not a child of /f.
        assert!(matches!(sym(&mut sm, 5, "/f2"), ClientResponse::Success));
        assert_eq!(sm.files["/f2"].symlink_target.as_deref(), Some("/t"));
    }

    #[test]
    fn link_refuses_existing_dest_and_allows_fresh() {
        let mut sm = StateMachineData::default();
        create(&mut sm, 1, "/src");
        create(&mut sm, 2, "/dst");
        create(&mut sm, 3, "/d/x");
        let link = |sm: &mut StateMachineData, i, d: &str| apply_request(sm, &lid(i),
            &ClientRequest::CreateLink { source: "/src".into(), dest: d.into(), now_ms: 0 });
        assert_eq!(err_msg(link(&mut sm, 4, "/dst")), "Link: destination '/dst' already exists");
        assert_eq!(err_msg(link(&mut sm, 5, "/d")), "Link: destination '/d' already exists");
        assert!(matches!(link(&mut sm, 6, "/new"), ClientResponse::Success));
        assert!(sm.files.contains_key("/new"));
    }

    fn rename(sm: &mut StateMachineData, i: u64, from: &str, to: &str) -> ClientResponse {
        apply_request(sm, &lid(i),
            &ClientRequest::RenameFile { from: from.into(), to: to.into(), now_ms: 0 })
    }

    #[test]
    fn rename_directory_onto_regular_file_is_refused() {
        let mut sm = StateMachineData::default();
        create(&mut sm, 1, "/a/x");
        create(&mut sm, 2, "/f");
        assert_eq!(err_msg(rename(&mut sm, 3, "/a", "/f")),
            "RenameFile: destination '/f' is not a directory");
        assert!(sm.files.contains_key("/a/x") && sm.files.contains_key("/f"));
    }

    #[test]
    fn rename_file_onto_directory_is_refused() {
        let mut sm = StateMachineData::default();
        create(&mut sm, 1, "/f");
        create(&mut sm, 2, "/d/x");
        assert_eq!(err_msg(rename(&mut sm, 3, "/f", "/d")),
            "RenameFile: destination '/d' is a directory");
        assert!(sm.files.contains_key("/f") && sm.files.contains_key("/d/x"));
    }

    #[test]
    fn rename_file_over_file_and_self_still_work() {
        let mut sm = StateMachineData::default();
        create(&mut sm, 1, "/f");
        create(&mut sm, 2, "/g");
        assert!(matches!(rename(&mut sm, 3, "/f", "/f"), ClientResponse::Success));
        assert!(matches!(rename(&mut sm, 4, "/f", "/g"), ClientResponse::Success));
        assert!(!sm.files.contains_key("/f") && sm.files.contains_key("/g"));
    }

    fn row(idx: u64, hash: &str, node: &str, disk: u32) -> ChunkLocationEntry {
        ChunkLocationEntry {
            chunk_index: idx, chunk_hash: hash.into(), node_id: node.into(),
            disk_id: disk, size_bytes: 10,
        }
    }

    fn upd(sm: &mut StateMachineData, i: u64, lease: &str, rows: Vec<ChunkLocationEntry>) -> ClientResponse {
        apply_request(sm, &lid(i), &ClientRequest::UpdateChunkLocations {
            path: "/f".into(), chunks: rows, lease_id: lease.into(), now_ms: 0,
        })
    }

    /// Rows produced by the in-place apply merges survive a snapshot
    /// round-trip and equal the old (reference) merge output.
    #[tokio::test]
    async fn in_place_merge_snapshot_loads_and_equals_reference() {
        use openraft::storage::{RaftSnapshotBuilder, RaftStateMachine};
        let dir = tempfile::tempdir().expect("tempdir");
        let mut want: Vec<ChunkLocationEntry> = Vec::new();
        {
            let mut store = StateMachineStore::open_at(dir.path()).expect("open");
            {
                let mut sm = store.data.write().await;
                create(&mut sm, 1, "/f");
                apply_request(&mut sm, &lid(2), &ClientRequest::AcquireWriteLease {
                    path: "/f".into(), owner_id: "w".into(), lease_ms: 60_000, now_ms: 1,
                    proposed_lease_id: "L".into(),
                });
                let steps: Vec<(&str, Vec<ChunkLocationEntry>)> = vec![
                    ("L", vec![row(3, "c", "2", 0), row(0, "a", "1", 0), row(3, "c", "1", 1)]),
                    ("L", vec![row(1, "b", "1", 0), row(2, "z", "1", 0)]),
                    ("", vec![row(0, "a", "3", 0), row(3, "c", "1", 0), row(2, "z", "0", 1)]),
                    ("L", vec![row(2, "y", "2", 0), row(2, "y", "1", 0)]),
                    ("", vec![row(1, "b", "1", 0), row(1, "b", "3", 1)]),
                ];
                for (i, (lease, rows)) in steps.into_iter().enumerate() {
                    want = if lease.is_empty() {
                        union_chunk_location_entries(&want, &rows).0
                    } else {
                        merge_chunk_location_entries(&want, &rows)
                    };
                    upd(&mut sm, 10 + i as u64, lease, rows);
                    assert_eq!(sm.files["/f"].chunks, want, "step {i}");
                }
            }
            let mut builder = store.get_snapshot_builder().await;
            builder.build_snapshot().await.expect("build");
        }
        let restored = StateMachineStore::open_at(dir.path()).expect("reopen");
        assert_eq!(restored.data.read().await.files["/f"].chunks, want);
    }

    #[test]
    fn union_adds_missing_replica_and_never_removes() {
        let existing = vec![row(0, "h", "1", 0), row(0, "h", "2", 0)];
        // Stale incoming only knows node 1 and a new node 3.
        let (out, skipped) = union_chunk_location_entries(
            &existing, &[row(0, "h", "1", 0), row(0, "h", "3", 0)]);
        assert!(skipped.is_empty());
        let nodes: Vec<&str> = out.iter().map(|e| e.node_id.as_str()).collect();
        assert_eq!(nodes, ["1", "2", "3"], "node 2 kept, node 3 added");
    }

    #[test]
    fn union_skips_stale_hash_and_rowless_index() {
        let existing = vec![row(0, "new", "1", 0)];
        let (out, skipped) = union_chunk_location_entries(
            &existing, &[row(0, "old", "2", 0), row(1, "x", "2", 0)]);
        assert_eq!(skipped, vec![0, 1], "old-hash write to index 0 and a row-less index 1 are stale");
        assert_eq!(out, vec![row(0, "new", "1", 0)]);
    }

    /// Lease-less commit that is (partly) stale is reported as an error, adds
    /// the valid rows, and never grows logical_size.
    #[test]
    fn lease_less_stale_commit_errors_and_never_grows_extent() {
        let mut sm = StateMachineData::default();
        create(&mut sm, 1, "/f");
        sm.files.get_mut("/f").unwrap().chunks = vec![row(0, "h", "1", 0)];
        // Rowless index 5 (e.g. truncated since the writer read) is skipped.
        let r = upd(&mut sm, 3, "", vec![row(0, "h", "2", 0), row(5, "z", "1", 0)]);
        assert!(err_msg(r).contains("stale lease-less commit"));
        assert_eq!(sm.files["/f"].chunks, vec![row(0, "h", "1", 0), row(0, "h", "2", 0)]);
        // Fully valid lease-less write still succeeds.
        assert!(matches!(upd(&mut sm, 4, "", vec![row(0, "h", "3", 0)]), ClientResponse::Success));
        // Extent: lease-less never grows, leased does.
        sm.files.get_mut("/f").unwrap().logical_size = Some(1);
        upd(&mut sm, 5, "", vec![row(0, "h", "4", 0)]);
        assert_eq!(sm.files["/f"].logical_size, Some(1), "lease-less write must not grow the extent");
        apply_request(&mut sm, &lid(6), &ClientRequest::AcquireWriteLease {
            path: "/f".into(), owner_id: "w".into(), lease_ms: 60_000, now_ms: 1,
            proposed_lease_id: "L".into(),
        });
        upd(&mut sm, 7, "L", vec![row(0, "h", "4", 0)]);
        assert_eq!(sm.files["/f"].logical_size, Some(10), "leased write grows to row extent");
    }

    /// RemoveCorruptChunkReplicas keeps the last row of an index (with an
    /// audit row) and still drops one of several.
    #[test]
    fn remove_corrupt_keeps_last_row_and_audits() {
        let mut sm = StateMachineData::default();
        create(&mut sm, 1, "/f");
        sm.files.get_mut("/f").unwrap().chunks = vec![row(0, "h", "1", 0), row(0, "h", "2", 0), row(1, "g", "1", 0)];
        let rm = |sm: &mut StateMachineData, i, n: &str, h: &str| apply_request(sm, &lid(i),
            &ClientRequest::RemoveCorruptChunkReplicas {
                now_ms: 0, node_id: n.into(), chunk_hashes: vec![h.into()], disk_id: 0 });
        rm(&mut sm, 10, "1", "g");
        assert_eq!(sm.files["/f"].chunks.len(), 3, "sole row of index 1 kept");
        assert!(sm.audit_log.iter().any(|a| a.summary.contains("kept 1 chunk index")));
        rm(&mut sm, 11, "1", "h");
        assert_eq!(sm.files["/f"].chunks.len(), 2, "one of two rows at index 0 dropped");
        rm(&mut sm, 12, "2", "h");
        assert_eq!(sm.files["/f"].chunks.len(), 2, "last row of index 0 kept");
    }

    /// Property: over random op sequences, RemoveCorruptChunkReplicas never
    /// empties a chunk index.
    #[test]
    fn remove_corrupt_never_empties_an_index_property() {
        for seed in 1..=40u64 {
            let mut x = seed.wrapping_mul(0x9E37_79B9_7F4A_7C15) | 1;
            let mut rnd = move |n: u64| {
                x ^= x << 13; x ^= x >> 7; x ^= x << 17;
                x % n
            };
            let mut sm = StateMachineData::default();
            create(&mut sm, 1, "/f");
            let mut rows = Vec::new();
            for i in 0..3u64 {
                for _ in 0..(rnd(3) + 1) {
                    let r = row(i, &format!("h{i}"), &rnd(3).to_string(), rnd(2) as u32);
                    if !rows.contains(&r) { rows.push(r); }
                }
            }
            sm.files.get_mut("/f").unwrap().chunks = rows.clone();
            for k in 0..30u64 {
                let disk = if rnd(3) == 0 { legacy_disk_id() } else { rnd(2) as u32 };
                apply_request(&mut sm, &lid(10 + k), &ClientRequest::RemoveCorruptChunkReplicas {
                    now_ms: 0, node_id: rnd(3).to_string(),
                    chunk_hashes: vec![format!("h{}", rnd(3))], disk_id: disk });
                for i in 0..3u64 {
                    let had = rows.iter().any(|r| r.chunk_index == i);
                    let has = sm.files["/f"].chunks.iter().any(|r| r.chunk_index == i);
                    assert!(!had || has, "seed {seed}: index {i} emptied");
                }
            }
        }
    }

    #[test]
    fn lease_less_write_keeps_removed_row_removed_and_lease_write_replaces() {
        let mut sm = StateMachineData::default();
        create(&mut sm, 1, "/f");
        sm.files.get_mut("/f").unwrap().chunks = vec![row(0, "h", "1", 0), row(0, "h", "2", 0)];
        apply_request(&mut sm, &lid(3), &ClientRequest::RemoveChunkLocation {
            path: "/f".into(), chunk_index: 0, node_id: "2".into(), disk_id: 0,
            enforce_anchor: None, now_ms: 0,
        });
        assert_eq!(sm.files["/f"].chunks.len(), 1);
        // Stale scheduler write built from the pre-removal read of node 1 only.
        upd(&mut sm, 4, "", vec![row(0, "h", "1", 0)]);
        assert_eq!(sm.files["/f"].chunks, vec![row(0, "h", "1", 0)]);
        // Lease holder keeps replace semantics (new content).
        apply_request(&mut sm, &lid(5), &ClientRequest::AcquireWriteLease {
            path: "/f".into(), owner_id: "w".into(), lease_ms: 60_000, now_ms: 1,
            proposed_lease_id: "L".into(),
        });
        upd(&mut sm, 6, "L", vec![row(0, "h2", "3", 0)]);
        assert_eq!(sm.files["/f"].chunks, vec![row(0, "h2", "3", 0)]);
    }

    /// Property: over random op sequences a lease-less write never removes a
    /// row, never changes the hash set of an index that already had rows, and
    /// only adds rows it carried.
    #[test]
    fn lease_less_update_property_over_random_ops() {
        for seed in 1..=40u64 {
            let mut x = seed.wrapping_mul(0x9E37_79B9_7F4A_7C15) | 1;
            let mut rnd = move |n: u64| {
                x ^= x << 13; x ^= x >> 7; x ^= x << 17;
                x % n
            };
            let mut sm = StateMachineData::default();
            create(&mut sm, 1, "/f");
            let mut idx = 10u64;
            for _ in 0..60 {
                idx += 1;
                let pre = sm.files["/f"].chunks.clone();
                match rnd(4) {
                    0 | 1 => {
                        let n = rnd(3) + 1;
                        let rows: Vec<_> = (0..n).map(|_| row(
                            rnd(2), &format!("h{}", rnd(2)), &rnd(3).to_string(), rnd(2) as u32)).collect();
                        upd(&mut sm, idx, "", rows.clone());
                        let post = &sm.files["/f"].chunks;
                        for r in &pre {
                            assert!(post.contains(r), "seed {seed}: lease-less write removed {r:?}");
                        }
                        for r in post.iter().filter(|r| !pre.contains(r)) {
                            assert!(rows.contains(r), "seed {seed}: row {r:?} not carried by the write");
                        }
                        for i in 0..2u64 {
                            let hs = |v: &[ChunkLocationEntry]| {
                                let mut h: Vec<_> = v.iter().filter(|e| e.chunk_index == i)
                                    .map(|e| e.chunk_hash.clone()).collect();
                                h.sort(); h.dedup(); h
                            };
                            if !hs(&pre).is_empty() {
                                assert_eq!(hs(&pre), hs(post), "seed {seed}: hash changed at {i}");
                            }
                        }
                    }
                    2 => {
                        if let Some(r) = pre.first() {
                            apply_request(&mut sm, &lid(idx), &ClientRequest::RemoveChunkLocation {
                                path: "/f".into(), chunk_index: r.chunk_index,
                                node_id: r.node_id.clone(), disk_id: r.disk_id,
                                enforce_anchor: None, now_ms: 0,
                            });
                            let post = &sm.files["/f"].chunks;
                            assert!(post.len() <= pre.len(), "seed {seed}");
                        }
                    }
                    _ => {
                        // Lease rewrite: replace semantics with fresh content.
                        apply_request(&mut sm, &lid(idx), &ClientRequest::AcquireWriteLease {
                            path: "/f".into(), owner_id: "w".into(), lease_ms: 60_000, now_ms: 1,
                            proposed_lease_id: "L".into(),
                        });
                        let rows = vec![row(rnd(2), &format!("h{}", rnd(2)), &rnd(3).to_string(), 0)];
                        let r = upd(&mut sm, idx, "L", rows.clone());
                        if matches!(r, ClientResponse::Success) {
                            for e in &rows {
                                assert!(sm.files["/f"].chunks.contains(e), "seed {seed}");
                            }
                        }
                        apply_request(&mut sm, &lid(idx), &ClientRequest::ReleaseWriteLease { now_ms: 0,
                            path: "/f".into(), lease_id: "L".into(),
                        });
                    }
                }
            }
        }
    }

    #[test]
    fn push_audit_capped_drops_oldest_first() {
        let mut log = std::collections::VecDeque::new();
        for n in 0..(MAX_AUDIT_LOG_ENTRIES as u64 + 3) {
            push_audit_capped(&mut log, AuditEntry {
                log_index: n, timestamp_ms: 0, summary: String::new(),
                actor: String::new(), action: String::new(), target: String::new(),
                result: String::new(), origin_manager: String::new(),
            });
        }
        assert_eq!(log.len(), MAX_AUDIT_LOG_ENTRIES);
        assert_eq!(log.front().unwrap().log_index, 3);
        assert_eq!(log.back().unwrap().log_index, MAX_AUDIT_LOG_ENTRIES as u64 + 2);
    }

    #[test]
    fn audit_timestamp_is_independent_of_wall_clock() {
        let reqs = vec![
            // Legacy entry (now_ms 0) falls back to the log index; a
            // proposer-stamped one is used as the audit timestamp.
            ClientRequest::DeleteVolume { now_ms: 0, volume_id: "v".into() },
            ClientRequest::DeleteVolume { now_ms: 888, volume_id: "v".into() },
            ClientRequest::DeleteFile { now_ms: 999, path: "/f".into() },
            ClientRequest::SweepExpiredLeases { now_ms: 777, sweep_posix_locks: true },
            ClientRequest::AcquireWriteLease {
                path: "/f".into(), owner_id: "o".into(), lease_ms: 10, now_ms: 555,
                proposed_lease_id: "L".into(),
            },
        ];
        for (n, req) in reqs.iter().enumerate() {
            let apply_once = || {
                let mut sm = StateMachineData::default();
                create(&mut sm, 1, "/f");
                if matches!(req, ClientRequest::SweepExpiredLeases { .. }) {
                    apply_request(&mut sm, &lid(2), &ClientRequest::AcquireWriteLease {
                        path: "/f".into(), owner_id: "o".into(), lease_ms: 1, now_ms: 1,
                        proposed_lease_id: "old".into(),
                    });
                }
                apply_request(&mut sm, &lid(40 + n as u64), req);
                sm.audit_log.iter().filter(|e| e.log_index == 40 + n as u64)
                    .map(|e| (e.summary.clone(), e.timestamp_ms)).collect::<Vec<_>>()
            };
            let first = apply_once();
            std::thread::sleep(std::time::Duration::from_millis(5));
            let second = apply_once();
            assert!(!first.is_empty(), "{req:?} must audit");
            assert_eq!(first, second, "{req:?}");
            let expect = match req {
                ClientRequest::SweepExpiredLeases { .. } => 777,
                ClientRequest::AcquireWriteLease { .. } => 555,
                ClientRequest::DeleteVolume { now_ms: 888, .. } => 888,
                ClientRequest::DeleteFile { .. } => 999,
                _ => 40 + n as u64,
            };
            assert!(first.iter().all(|(_, t)| *t == expect), "{first:?} want {expect}");
        }
    }
}

/// `FileMetadata.ever_committed`: only a file whose committed data is gone
/// counts as missing chunks, not an empty or never-written sparse file.
#[cfg(test)]
mod ever_committed_tests {
    use super::*;

    const MIB: u64 = 1024 * 1024;

    fn lid(idx: u64) -> LogId {
        use openraft::vote::RaftLeaderId;
        let leader = openraft::impls::leader_id_adv::LeaderId::<u64, u64>::new(0, 0);
        openraft::LogId::new(leader, idx)
    }

    fn create(sm: &mut StateMachineData, idx: u64, path: &str) {
        let r = apply_request(sm, &lid(idx), &ClientRequest::CreateFile {
            now_ms: 0, path: path.into(), volume_id: "default".into(), chunk_size: MIB,
        });
        assert!(matches!(r, ClientResponse::FileCreated { .. }), "{r:?}");
    }

    fn row(i: u64) -> ChunkLocationEntry {
        ChunkLocationEntry {
            chunk_index: i, chunk_hash: format!("h{i}"), node_id: "7".into(),
            disk_id: 1, size_bytes: MIB,
        }
    }

    fn commit(sm: &mut StateMachineData, idx: u64, path: &str, i: u64) {
        let r = apply_request(sm, &lid(idx), &ClientRequest::UpdateChunkLocations {
            path: path.into(), chunks: vec![row(i)], lease_id: "seed".into(), now_ms: 0,
        });
        assert_eq!(r, ClientResponse::Success);
    }

    fn set_size(sm: &mut StateMachineData, idx: u64, path: &str, new_size: u64) {
        let r = apply_request(sm, &lid(idx), &ClientRequest::SetFileSize {
            path: path.into(), new_size, now_ms: 0,
        });
        assert_eq!(r, ClientResponse::Success);
    }

    #[test]
    fn empty_file_is_not_missing() {
        let mut sm = StateMachineData::default();
        create(&mut sm, 1, "/e");
        let f = &sm.files["/e"];
        assert_eq!(f.ever_committed, Some(false));
        assert!(!f.committed_data_missing());
        set_size(&mut sm, 2, "/e", 0); // `: > e` / O_TRUNC
        assert!(!sm.files["/e"].committed_data_missing());
    }

    #[test]
    fn sparse_never_written_file_is_not_missing() {
        let mut sm = StateMachineData::default();
        create(&mut sm, 1, "/s");
        set_size(&mut sm, 2, "/s", 10 * MIB); // truncate -s 10M
        let f = &sm.files["/s"];
        assert_eq!((f.ever_committed, f.logical_size), (Some(false), Some(10 * MIB)));
        assert!(!f.committed_data_missing());
    }

    #[test]
    fn written_file_that_lost_its_chunks_is_missing() {
        let mut sm = StateMachineData::default();
        create(&mut sm, 1, "/w");
        seed_lease(&mut sm, "/w");
        commit(&mut sm, 2, "/w", 0);
        assert_eq!(sm.files["/w"].ever_committed, Some(true));
        assert!(!sm.files["/w"].committed_data_missing(), "chunks present");
        // BP-4: rows gone (e.g. GC) => realized data loss, with or without a
        // logical size set.
        sm.files.get_mut("/w").unwrap().chunks.clear();
        assert!(sm.files["/w"].committed_data_missing());
    }

    #[test]
    fn lease_less_union_onto_existing_rows_marks_committed() {
        let mut sm = StateMachineData::default();
        create(&mut sm, 1, "/u");
        seed_lease(&mut sm, "/u");
        commit(&mut sm, 2, "/u", 0);
        // A legacy entry (marker unknown) re-registered by the scheduler.
        sm.files.get_mut("/u").unwrap().ever_committed = None;
        let mut copy = row(0);
        copy.node_id = "8".into();
        let r = apply_request(&mut sm, &lid(3), &ClientRequest::UpdateChunkLocations {
            path: "/u".into(), chunks: vec![copy], lease_id: String::new(), now_ms: 0,
        });
        assert_eq!(r, ClientResponse::Success);
        assert_eq!(sm.files["/u"].ever_committed, Some(true));
    }

    #[test]
    fn rename_and_link_keep_the_marker() {
        let mut sm = StateMachineData::default();
        create(&mut sm, 1, "/a");
        seed_lease(&mut sm, "/a");
        commit(&mut sm, 2, "/a", 0);
        apply_request(&mut sm, &lid(3), &ClientRequest::RenameFile {
            from: "/a".into(), to: "/b".into(), now_ms: 0,
        });
        apply_request(&mut sm, &lid(4), &ClientRequest::CreateLink {
            source: "/b".into(), dest: "/c".into(), now_ms: 0,
        });
        assert_eq!(sm.files["/b"].ever_committed, Some(true));
        assert_eq!(sm.files["/c"].ever_committed, Some(true));
    }

    #[test]
    fn truncate_to_zero_resets_marker_and_regrow_is_not_missing() {
        let mut sm = StateMachineData::default();
        create(&mut sm, 1, "/t");
        seed_lease(&mut sm, "/t");
        commit(&mut sm, 2, "/t", 0);
        set_size(&mut sm, 3, "/t", 0);
        assert_eq!(sm.files["/t"].ever_committed, Some(false));
        set_size(&mut sm, 4, "/t", 10 * MIB); // truncate 0 then sparse grow
        assert!(!sm.files["/t"].committed_data_missing());
        // Boundary: shrinking to 1 byte keeps chunk 0 and the marker.
        commit(&mut sm, 5, "/t", 0);
        set_size(&mut sm, 6, "/t", 1);
        assert_eq!(sm.files["/t"].ever_committed, Some(true));
    }

    #[test]
    fn truncate_dropping_every_chunk_is_not_missing() {
        let mut sm = StateMachineData::default();
        create(&mut sm, 1, "/c3");
        seed_lease(&mut sm, "/c3");
        commit(&mut sm, 2, "/c3", 3); // only chunk 3 written (sparse head)
        set_size(&mut sm, 3, "/c3", 1);
        assert!(!sm.files["/c3"].committed_data_missing());
    }

    #[test]
    fn truncate_of_an_already_lost_file_stays_missing() {
        let mut sm = StateMachineData::default();
        create(&mut sm, 1, "/lost");
        seed_lease(&mut sm, "/lost");
        commit(&mut sm, 2, "/lost", 0);
        sm.files.get_mut("/lost").unwrap().chunks.clear(); // rows already gone
        set_size(&mut sm, 3, "/lost", 1);
        assert!(sm.files["/lost"].committed_data_missing(), "truncate must not hide loss");
    }

    #[test]
    fn legacy_entry_excluded_only_with_explicit_zero_size() {
        let mut f = FileMetadata {
            path: "/l".into(), volume_id: "default".into(), chunk_size: MIB, chunks: vec![],
            created_at_ms: 0, updated_at_ms: 0, symlink_target: None, logical_size: None,
            is_directory: false, cache_cooldown_until_ms: Default::default(),
            ever_committed: None,
        };
        assert!(f.committed_data_missing(), "legacy, size unset: may be real loss");
        f.logical_size = Some(0);
        assert!(!f.committed_data_missing(), "legacy, size 0");
        f.logical_size = Some(1);
        assert!(f.committed_data_missing(), "legacy, size 1 without chunks");
        f.is_directory = true;
        assert!(!f.committed_data_missing(), "directory never counts");
        f.is_directory = false;
        f.symlink_target = Some("/x".into());
        assert!(!f.committed_data_missing(), "symlink never counts");
    }

    #[test]
    fn create_stamps_proposer_time_and_falls_back_to_log_index() {
        let mut sm = StateMachineData::default();
        apply_request(&mut sm, &lid(5), &ClientRequest::CreateFile {
            now_ms: 1_700_000_000_000, path: "/n".into(), volume_id: "default".into(), chunk_size: MIB,
        });
        let f = &sm.files["/n"];
        assert_eq!((f.created_at_ms, f.updated_at_ms), (1_700_000_000_000, 1_700_000_000_000));
        create(&mut sm, 6, "/legacy");
        assert_eq!(sm.files["/legacy"].created_at_ms, 6, "now_ms 0 -> log index");
    }

    /// Random op sequences: replay is deterministic and the marker is
    /// `Some(true)` on every regular file that holds chunks.
    #[test]
    fn random_ops_are_deterministic_and_keep_marker_invariant() {
        let paths = ["/p0", "/p1", "/p2"];
        let mut seed: u64 = 0x9E37_79B9_7F4A_7C15;
        let mut next = || {
            seed = seed.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
            seed >> 33
        };
        let mut ops = Vec::new();
        for _ in 0..400 {
            let p = paths[(next() % 3) as usize].to_string();
            ops.push(match next() % 5 {
                0 => ClientRequest::CreateFile {
                    now_ms: 0, path: p, volume_id: "default".into(), chunk_size: MIB,
                },
                1 | 2 => ClientRequest::UpdateChunkLocations {
                    path: p, chunks: vec![row(next() % 4)], lease_id: String::new(), now_ms: 0,
                },
                3 => ClientRequest::SetFileSize { path: p, new_size: (next() % 5) * MIB, now_ms: 0 },
                _ => ClientRequest::DeleteFile { path: p, now_ms: 0 },
            });
        }
        let run = |check: bool| {
            let mut sm = StateMachineData::default();
            for (i, op) in ops.iter().enumerate() {
                // Lease-less writes cannot add rows; seed them with a lease.
                if let ClientRequest::UpdateChunkLocations { path, chunks, .. } = op {
                    if sm.files.contains_key(path) && next_bool(i) {
                        seed_lease(&mut sm, path);
                        apply_request(&mut sm, &lid(i as u64 + 1), &ClientRequest::UpdateChunkLocations {
                            path: path.clone(), chunks: chunks.clone(), lease_id: "seed".into(), now_ms: 0,
                        });
                        sm.write_leases.remove(path);
                        continue;
                    }
                }
                apply_request(&mut sm, &lid(i as u64 + 1), op);
                if check {
                    for f in sm.files.values() {
                        if !f.chunks.is_empty() {
                            assert_eq!(f.ever_committed, Some(true), "step {i}: {f:?}");
                        }
                        if f.ever_committed == Some(false) {
                            assert!(!f.committed_data_missing(), "step {i}");
                        }
                    }
                }
            }
            sm.files
        };
        assert_eq!(run(true), run(false), "replay must be deterministic");
    }

    fn next_bool(i: usize) -> bool {
        i % 3 != 0
    }
}
