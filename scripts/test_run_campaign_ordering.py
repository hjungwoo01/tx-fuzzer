from __future__ import annotations

import copy
import random
from typing import Any, Dict, List

from scripts.run_campaign import (
    _can_move_left_preserving_client_order,
    mutate_reorder_smart,
)


def _base_trace() -> List[Dict[str, Any]]:
    # Deliberately dense same-key access pattern to maximize smart-reorder candidates.
    return [
        {"eid": "a1", "client_id": 0, "sql": "SELECT v FROM kv WHERE k=$1", "args": [1], "type": "invoke"},
        {"eid": "b1", "client_id": 1, "sql": "SELECT v FROM kv WHERE k=$1", "args": [1], "type": "invoke"},
        {"eid": "b2", "client_id": 1, "sql": "UPDATE kv SET v=$1 WHERE k=$2", "args": [1001, 1], "type": "invoke"},
        {"eid": "a2", "client_id": 0, "sql": "UPDATE kv SET v=$1 WHERE k=$2", "args": [2001, 1], "type": "invoke"},
        {"eid": "b3", "client_id": 1, "sql": "SELECT v FROM kv WHERE k=$1", "args": [1], "type": "invoke"},
        {"eid": "a3", "client_id": 0, "sql": "SELECT v FROM kv WHERE k=$1", "args": [1], "type": "invoke"},
    ]


def _client_local_order(trace: List[Dict[str, Any]]) -> Dict[int, List[str]]:
    out: Dict[int, List[str]] = {}
    for ev in trace:
        cid = int(ev["client_id"])
        out.setdefault(cid, []).append(str(ev["eid"]))
    return out


def test_can_move_guard_blocks_same_client_overtake() -> None:
    trace = _base_trace()
    # b3 (idx 4) cannot jump ahead of b1/b2 (same client 1).
    assert _can_move_left_preserving_client_order(trace, from_idx=4, to_idx=0) is False
    # a2 (idx 3) can move to idx 1 because it only crosses client 1 events.
    assert _can_move_left_preserving_client_order(trace, from_idx=3, to_idx=1) is True


def test_mutate_reorder_smart_preserves_client_local_order() -> None:
    base = _base_trace()
    base_order = _client_local_order(base)
    base_eids = sorted(str(ev["eid"]) for ev in base)

    saw_change = False
    for seed in range(200):
        trace = copy.deepcopy(base)
        changed = mutate_reorder_smart(trace, random.Random(seed), window=len(trace))
        if changed:
            saw_change = True
        assert _client_local_order(trace) == base_order
        assert sorted(str(ev["eid"]) for ev in trace) == base_eids

    assert saw_change is True
