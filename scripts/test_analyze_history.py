import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
SCRIPTS_DIR = ROOT / "scripts"
sys.path.append(str(SCRIPTS_DIR))

import analyze_history as ah 


def build_txn_events(txn_id, process, ops, status="ok"):
    """Construct invoke/terminal event pairs matching the analyzer's expectations."""
    invoke = {"type": "invoke", "f": ":txn", "process": process, "value": ops, "txn": txn_id}
    terminal = {
        "type": status,
        "f": ":txn",
        "process": process,
        "value": ops,
        "txn": txn_id,
    }
    return [invoke, terminal]


def analyze(events):
    txns = ah.extract_transactions(events)
    return ah.analyze_transactions(txns)


def test_unique_wr_edge():
    events = []
    events += build_txn_events("t1", 0, [[":w", "k", 1]])
    events += build_txn_events("t2", 1, [[":r", "k", 1]])
    result = analyze(events)
    wr_edges = result.edges["wr"]
    assert len(wr_edges) == 1
    edge = wr_edges[0]
    assert edge.source == "t1" and edge.target == "t2"
    assert edge.ambiguous is False
    assert not result.cycles


def test_rw_antidependency():
    events = []
    events += build_txn_events("t1", 0, [[":r", "k", None]])
    events += build_txn_events("t2", 1, [[":w", "k", 5]])
    result = analyze(events)
    rw_edges = result.edges["rw"]
    assert len(rw_edges) == 1
    assert rw_edges[0].source == "t1" and rw_edges[0].target == "t2"
    # Initial state should seed a WR edge
    wr_sources = {e.source for e in result.edges["wr"]}
    assert "INIT" in wr_sources


def test_ww_chain():
    events = []
    events += build_txn_events("t1", 0, [[":w", "k", 1]])
    events += build_txn_events("t2", 1, [[":w", "k", 2]])
    result = analyze(events)
    ww_edges = result.edges["ww"]
    assert len(ww_edges) == 1
    assert ww_edges[0].source == "t1" and ww_edges[0].target == "t2"


def test_duplicate_write_values_are_ambiguous():
    events = []
    events += build_txn_events("t1", 0, [[":w", "k", 1]])
    events += build_txn_events("t2", 1, [[":w", "k", 1]])
    events += build_txn_events("t3", 2, [[":r", "k", 1]])
    result = analyze(events)
    wr_edges = result.edges["wr"]
    assert len(wr_edges) == 2
    assert all(edge.target == "t3" for edge in wr_edges)
    assert all(edge.ambiguous for edge in wr_edges)


def test_initial_state_wr_edge():
    events = build_txn_events("t1", 0, [[":r", "k", None]])
    result = analyze(events)
    wr_edges = result.edges["wr"]
    assert len(wr_edges) == 1
    assert wr_edges[0].source == "INIT"
    assert wr_edges[0].target == "t1"


def test_dirty_read_flagged():
    events = []
    events += build_txn_events("t1", 0, [[":w", "k", 9]], status="fail")
    events += build_txn_events("t2", 1, [[":r", "k", 9]])
    result = analyze(events)
    wr_edges = result.edges["wr"]
    assert len(wr_edges) == 1
    assert result.anomalies.get("dirty_read")


def test_near_cycle_feedback_present():
    # T1 writes k1, T2 reads k1 and writes k2, T3 reads k2 -> A->B->C path missing C->A
    events = []
    events += build_txn_events("t1", 0, [[":w", "k1", 1]])
    events += build_txn_events("t2", 1, [[":r", "k1", 1], [":w", "k2", 2]])
    events += build_txn_events("t3", 2, [[":r", "k2", 2]])
    result = analyze(events)
    assert any(nc.start == "t1" and nc.via == "t2" and nc.end == "t3" for nc in result.feedback.near_cycles)
    assert result.feedback.suggestions


def test_near_cycle_scoring_present():
    events = []
    events += build_txn_events("t1", 0, [[":w", "k1", 1]])
    events += build_txn_events("t2", 1, [[":r", "k1", 1], [":w", "k2", 2]])
    events += build_txn_events("t3", 2, [[":r", "k2", 2]])
    result = analyze(events)
    assert result.feedback.near_cycles
    top = result.feedback.near_cycles[0]
    assert top.path_count >= 1
    assert top.cycle_gain >= 1


def test_adjacency_export_dense_and_sparse(tmp_path):
    events = []
    events += build_txn_events("t1", 0, [[":w", "k", 1]])
    events += build_txn_events("t2", 1, [[":r", "k", 1]])
    result = analyze(events)

    dense_path = tmp_path / "dense.tsv"
    ah.export_adjacency_matrix(result.graph, dense_path, dense_limit=10)
    dense_lines = dense_path.read_text().strip().splitlines()
    assert dense_lines[0].split("\t")[1:] == ["t1", "t2"]
    assert "t1\t0\twr" in dense_lines

    sparse_path = tmp_path / "sparse.tsv"
    ah.export_adjacency_matrix(result.graph, sparse_path, dense_limit=1)
    sparse_lines = sparse_path.read_text().strip().splitlines()
    assert sparse_lines[0].startswith("# Sparse adjacency list")
    assert any(line.startswith("t1\tt2\t") for line in sparse_lines[1:])
