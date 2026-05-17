"""Unit tests for scripts.update_golden_domain_scores."""
import sys
from pathlib import Path

# Make scripts/ importable so the module's top-level `from constants` works
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "scripts"))

import importlib.util

_MODULE_PATH = Path(__file__).resolve().parents[1] / "scripts" / "update_golden_domain_scores.py"
_spec = importlib.util.spec_from_file_location("update_golden_domain_scores", _MODULE_PATH)
_mod = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(_mod)

classify_tiers = _mod.classify_tiers
collapse_to_domain_keys = _mod.collapse_to_domain_keys


def test_classify_tiers_all_batches_goes_to_t0():
    t0, t1, t2 = classify_tiers(
        {"a.com": {1, 2, 3, 4, 5}},
        [1, 2, 3, 4, 5],
    )
    assert t0 == {"a.com"}
    assert t1 == set()
    assert t2 == set()


def test_classify_tiers_last_two_consecutive_goes_to_t1():
    t0, t1, t2 = classify_tiers(
        {"a.com": {4, 5}},  # last two only
        [1, 2, 3, 4, 5],
    )
    assert t0 == set()
    assert t1 == {"a.com"}
    assert t2 == set()


def test_classify_tiers_partial_history_goes_to_t2():
    t0, t1, t2 = classify_tiers(
        {"a.com": {2, 3}},  # not last two, not all
        [1, 2, 3, 4, 5],
    )
    assert t0 == set()
    assert t1 == set()
    assert t2 == {"a.com"}


def test_classify_tiers_t1_does_not_overlap_t0():
    # Domain in ALL batches qualifies for both, but goes to T0 (highest).
    t0, t1, t2 = classify_tiers(
        {"a.com": {1, 2, 3, 4, 5}, "b.com": {4, 5}},
        [1, 2, 3, 4, 5],
    )
    assert t0 == {"a.com"}
    assert t1 == {"b.com"}
    assert "a.com" not in t1
    assert "a.com" not in t2


def test_classify_tiers_t1_requires_both_last_two():
    # Has last batch only — not T1, goes to T2.
    t0, t1, t2 = classify_tiers(
        {"a.com": {5}},
        [1, 2, 3, 4, 5],
    )
    assert t0 == set()
    assert t1 == set()
    assert t2 == {"a.com"}


def test_classify_tiers_t1_when_t0_extended_subset():
    # 4 and 5 plus extras — still goes to T0 if it's ALL batches; else T1.
    t0, t1, t2 = classify_tiers(
        {"a.com": {3, 4, 5}, "b.com": {1, 3, 4, 5}},
        [1, 2, 3, 4, 5],
    )
    # a.com missing 1,2 -> not T0; has 4,5 -> T1
    # b.com missing 2 -> not T0; has 4,5 -> T1
    assert t0 == set()
    assert t1 == {"a.com", "b.com"}


def test_classify_tiers_empty_batch_history_returns_empty():
    t0, t1, t2 = classify_tiers({"a.com": {1}}, [])
    assert t0 == set()
    assert t1 == set()
    assert t2 == set()


def test_classify_tiers_single_batch_history():
    # Only batch [1] exists. last_two would be {1}, so T1 requires presence in {1}.
    # T0 also requires {1}. Both match, T0 wins.
    t0, t1, t2 = classify_tiers({"a.com": {1}}, [1])
    assert t0 == {"a.com"}
    assert t1 == set()


def test_classify_tiers_t3_implicit_not_in_any_set():
    # Empty presence: not in any returned set
    t0, t1, t2 = classify_tiers(
        {"a.com": set()},
        [1, 2, 3, 4, 5],
    )
    assert t0 == set()
    assert t1 == set()
    assert t2 == set()


def test_collapse_to_domain_keys_collapses_non_split_subdomains():
    # Without split_subdomains, en.wikipedia.org and ja.wikipedia.org both
    # collapse to wikipedia.org.
    raw = {
        "en.wikipedia.org": {1, 2},
        "ja.wikipedia.org": {3, 4},
        "wikipedia.org": {5},
    }
    out = collapse_to_domain_keys(raw, split_subdomains=set())
    assert out == {"wikipedia.org": {1, 2, 3, 4, 5}}


def test_collapse_to_domain_keys_preserves_split_subdomains():
    # When en.wikipedia.org is listed in split_subdomains it must be kept as-is.
    raw = {
        "en.wikipedia.org": {1, 2},
        "ja.wikipedia.org": {3, 4},
        "wikipedia.org": {5},
    }
    out = collapse_to_domain_keys(raw, split_subdomains={"en.wikipedia.org"})
    assert out == {
        "en.wikipedia.org": {1, 2},
        "wikipedia.org": {3, 4, 5},  # ja.* collapses since not in split set
    }
