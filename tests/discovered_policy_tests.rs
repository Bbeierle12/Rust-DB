//! T4 differential (research plan §8.3): the REAL BufferPool actor, driven
//! message-by-message through the committed trace vectors, must reproduce
//! the research reference's eviction decisions EXACTLY — every victim, in
//! order, plus total disk reads and writes.
//!
//! The vectors (tests/fixtures/discovered_policy_vectors.json) were emitted
//! by the edge-loop `eviction` crate's replay, itself differentially locked
//! against the Rhai research scaffold. Clock contract: the reference replay
//! is 1-based per event, so this driver calls tick(i + 1) before delivering
//! event i, and answers every DiskRead synchronously within the same tick.

#![cfg(feature = "trace-capture")]

use rust_dst_db::config::{DatabaseConfig, PAGE_SIZE};
use rust_dst_db::storage::buffer_pool::BufferPool;
use rust_dst_db::traits::message::{ActorId, Message};
use rust_dst_db::traits::state_machine::StateMachine;
use std::collections::BTreeMap;

const CLIENT: ActorId = ActorId(9);
const DISK: ActorId = ActorId(2);

fn page_data(page_type: u8) -> Vec<u8> {
    vec![page_type] // byte 0 is the page-type tag evict_score reads
}

struct Counts {
    reads: u64,
    writes: u64,
}

/// Drive one trace through a fresh BufferPool; returns (counts, victims).
fn drive(trace: &serde_json::Value) -> (Counts, Vec<u64>) {
    let capacity = trace["capacity"].as_u64().unwrap() as usize;
    let events = trace["events"].as_array().unwrap();

    let mut cfg = DatabaseConfig::default();
    cfg.buffer_pool_pages = capacity;
    let mut pool = BufferPool::new(ActorId(1), DISK, &cfg);
    let file_id = cfg.btree_data_file_id;

    // Page types are stable per page; remember them so the synthetic disk
    // returns data with the right tag byte.
    let mut types: BTreeMap<u64, u8> = BTreeMap::new();
    let mut counts = Counts { reads: 0, writes: 0 };

    for (i, ev) in events.iter().enumerate() {
        let op = ev[0].as_u64().unwrap();
        let page_id = ev[1].as_u64().unwrap();
        let page_type = ev[2].as_u64().unwrap() as u8;
        types.insert(page_id, page_type);
        pool.tick((i + 1) as u64);

        let outgoing = if op == 1 {
            pool.receive(
                CLIENT,
                Message::BufPoolWritePage {
                    page_id,
                    data: page_data(page_type),
                },
            )
        } else {
            pool.receive(CLIENT, Message::BufPoolReadPage { page_id })
        }
        .unwrap_or_default();

        let mut pending_read = None;
        for (msg, _) in outgoing {
            match msg {
                Message::DiskWrite { .. } => counts.writes += 1,
                Message::DiskRead { offset, .. } => {
                    counts.reads += 1;
                    pending_read = Some(offset);
                }
                _ => {}
            }
        }

        // Synchronous disk: answer the miss within the same tick, and count
        // any eviction write-back the insertion triggers.
        if let Some(offset) = pending_read {
            let pid = offset / PAGE_SIZE as u64;
            let data = page_data(*types.get(&pid).expect("read of unknown page"));
            let outgoing = pool
                .receive(
                    DISK,
                    Message::DiskReadOk {
                        file_id,
                        offset,
                        data,
                    },
                )
                .unwrap_or_default();
            for (msg, _) in outgoing {
                if let Message::DiskWrite { .. } = msg {
                    counts.writes += 1;
                }
            }
        }
    }

    // Terminal flush: every lingering dirty page costs one write.
    let outgoing = pool.receive(CLIENT, Message::BufPoolFlush).unwrap_or_default();
    for (msg, _) in outgoing {
        if let Message::DiskWrite { .. } = msg {
            counts.writes += 1;
        }
    }

    (counts, pool.take_evictions())
}

#[test]
fn real_pool_reproduces_research_vectors_exactly() {
    let raw = std::fs::read_to_string(
        std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("tests/fixtures/discovered_policy_vectors.json"),
    )
    .unwrap();
    let doc: serde_json::Value = serde_json::from_str(&raw).unwrap();

    for trace in doc["traces"].as_array().unwrap() {
        let id = trace["id"].as_str().unwrap();
        let (counts, victims) = drive(trace);

        let expected_victims: Vec<u64> = trace["victims"]
            .as_array()
            .unwrap()
            .iter()
            .map(|v| v.as_u64().unwrap())
            .collect();
        assert_eq!(
            victims.len(),
            expected_victims.len(),
            "{id}: eviction count mismatch"
        );
        for (k, (got, want)) in victims.iter().zip(&expected_victims).enumerate() {
            assert_eq!(got, want, "{id}: victim #{k} diverged");
        }
        assert_eq!(
            counts.reads,
            trace["disk_reads"].as_u64().unwrap(),
            "{id}: disk reads"
        );
        assert_eq!(
            counts.writes,
            trace["disk_writes"].as_u64().unwrap(),
            "{id}: disk writes"
        );
    }
}
