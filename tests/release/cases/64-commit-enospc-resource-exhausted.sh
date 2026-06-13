#!/usr/bin/env bash
# CO-5-3: a commit-time disk-full (ENOSPC) on the data node's put path must
# surface as gRPC RESOURCE_EXHAUSTED (so the FUSE/client backs off and the
# write reroutes), not a generic Internal error. Pre-fix the DtfsError::Io
# from commit_chunk_from_tmp was flattened via Status::internal(e.to_string),
# losing the errno-28 signal.
#
# Reliably forcing ENOSPC mid-commit on a live data disk is invasive (fill a
# real disk to exactly trigger the rename/fsync no-space path, then recover),
# so the behaviour is gated by the data-node unit test
# `server::tests::co_5_3_commit_enospc_maps_to_resource_exhausted`
# (raw_os_error 28 -> ResourceExhausted; other errno -> Internal). This case
# is a KNOWN_ISSUE stub pinned to CO-5-3; promote it to a live disk-fill
# scenario if/when the suite grows a sacrificial small-disk data node.
source "$(dirname "$0")/../lib/common.sh"

case_cleanup() { :; }
trap case_cleanup EXIT

known_issue CO-5-3 "PLAN-CO-CODE-AUDIT.md — covered by data-node unit test \
co_5_3_commit_enospc_maps_to_resource_exhausted; no live disk-fill harness yet"
