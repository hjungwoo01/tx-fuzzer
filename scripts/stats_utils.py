"""
Pure-Python statistical helpers for the IsoTest evaluation.

Implemented because the project deliberately avoids scipy (heavyweight,
non-trivial to install in the multi-DB Docker images).  All routines use
``random`` and ``statistics`` from the standard library only.

Provided
--------
  bootstrap_ci(samples, level=0.95, n_resamples=10000, seed=0)
      Percentile bootstrap confidence interval for the mean.
  mann_whitney_u(a, b)
      Two-sided Mann–Whitney U test with normal approximation.
      Returns (U, z, p, effect_size_r) where r = |z| / sqrt(n).
  welch_t_test(a, b)
      Two-sided Welch's t-test with Welch–Satterthwaite df.
      Returns (t, df, p) — p computed via Student-t survival approximation.
  cohens_d(a, b)
      Pooled Cohen's d effect size.
  cliffs_delta(a, b)
      Cliff's delta non-parametric effect size in [-1, 1].

These are intentionally textbook implementations; for very small or
very large samples some approximations will be loose. Adequate for the
sample sizes typical in the IsoTest evaluation (3 ≤ n ≤ 50).
"""

from __future__ import annotations

import math
import random as _random
import statistics
from typing import List, Sequence, Tuple


def _erf(x: float) -> float:
    return math.erf(x)


def normal_sf(z: float) -> float:
    """Two-sided survival function under N(0, 1)."""
    return 1.0 - 0.5 * (1.0 + _erf(abs(z) / math.sqrt(2.0)))


def normal_two_sided_p(z: float) -> float:
    return 2.0 * normal_sf(z)


# ---------------------------------------------------------------------------
# Bootstrap CI
# ---------------------------------------------------------------------------

def bootstrap_ci(
    samples: Sequence[float],
    level: float = 0.95,
    n_resamples: int = 10000,
    seed: int = 0,
) -> Tuple[float, float, float]:
    """Percentile bootstrap CI for the mean.

    Returns (mean, ci_lo, ci_hi).
    """
    if not samples:
        return 0.0, 0.0, 0.0
    if len(samples) == 1:
        v = float(samples[0])
        return v, v, v
    rng = _random.Random(seed)
    n = len(samples)
    means: List[float] = []
    for _ in range(n_resamples):
        s = sum(samples[rng.randrange(n)] for _ in range(n))
        means.append(s / n)
    means.sort()
    alpha = (1.0 - level) / 2.0
    lo = means[int(alpha * n_resamples)]
    hi = means[min(n_resamples - 1, int((1.0 - alpha) * n_resamples))]
    return statistics.mean(samples), lo, hi


# ---------------------------------------------------------------------------
# Mann–Whitney U (two-sided, normal approximation, with tie correction)
# ---------------------------------------------------------------------------

def _ranks(values: Sequence[float]) -> List[float]:
    """Average-rank ranking (handles ties)."""
    indexed = sorted(enumerate(values), key=lambda p: p[1])
    ranks = [0.0] * len(values)
    i = 0
    while i < len(indexed):
        j = i
        while j + 1 < len(indexed) and indexed[j + 1][1] == indexed[i][1]:
            j += 1
        avg_rank = (i + j) / 2.0 + 1.0  # 1-based
        for k in range(i, j + 1):
            ranks[indexed[k][0]] = avg_rank
        i = j + 1
    return ranks


def mann_whitney_u(
    a: Sequence[float], b: Sequence[float]
) -> Tuple[float, float, float, float]:
    """Two-sided Mann–Whitney U test.

    Returns (U, z, p, effect_size_r). Effect size r = |z| / sqrt(n_total)
    is the rank-biserial-style magnitude (Rosenthal 1991).
    """
    n1, n2 = len(a), len(b)
    if n1 == 0 or n2 == 0:
        return 0.0, 0.0, 1.0, 0.0
    combined = list(a) + list(b)
    ranks = _ranks(combined)
    R1 = sum(ranks[:n1])
    U1 = R1 - n1 * (n1 + 1) / 2.0
    U2 = n1 * n2 - U1
    U = min(U1, U2)
    mu = n1 * n2 / 2.0
    # Tie correction (BMG1947)
    tally: dict = {}
    for v in combined:
        tally[v] = tally.get(v, 0) + 1
    tie_term = sum(t ** 3 - t for t in tally.values())
    n = n1 + n2
    sigma_sq = (n1 * n2 / 12.0) * (n + 1 - tie_term / (n * (n - 1) if n > 1 else 1))
    if sigma_sq <= 0:
        return U, 0.0, 1.0, 0.0
    z = (U - mu) / math.sqrt(sigma_sq)
    p = normal_two_sided_p(z)
    r = abs(z) / math.sqrt(n) if n > 0 else 0.0
    return U, z, max(0.0, min(1.0, p)), r


# ---------------------------------------------------------------------------
# Welch's t-test
# ---------------------------------------------------------------------------

def _student_t_sf_approx(t: float, df: float) -> float:
    """Two-sided survival function for the t distribution.

    Uses the Wilson–Hilferty approximation for df > 1; falls back to N(0,1)
    when df is large. Adequate for the typical n ≥ 3 sample sizes here.
    """
    if df <= 0:
        return 1.0
    if df > 30:
        return normal_two_sided_p(t)
    # Bailey (1980) approximation: x = (1 - 2/(9 df)) * (t^2/df)^(1/3)
    # then z* = (x - 0) * sqrt(9 df / (2 t^2 + ...)) — simpler is to
    # use Hill's approximation:
    z = math.copysign(
        math.sqrt(df * (math.exp(t * t * (df - 5.0 / 6.0) /
                                 ((df - 2.0 / 3.0) ** 2 + t * t * 0.04)) - 1.0)),
        t,
    ) if df > 2 else t
    return normal_two_sided_p(z)


def welch_t_test(
    a: Sequence[float], b: Sequence[float]
) -> Tuple[float, float, float]:
    """Two-sided Welch's t-test (unequal variance).

    Returns (t, df, p_value).
    """
    n1, n2 = len(a), len(b)
    if n1 < 2 or n2 < 2:
        return 0.0, 0.0, 1.0
    m1, m2 = statistics.mean(a), statistics.mean(b)
    v1, v2 = statistics.variance(a), statistics.variance(b)
    if v1 == 0 and v2 == 0:
        return float("inf") if m1 != m2 else 0.0, 0.0, 0.0 if m1 != m2 else 1.0
    se = math.sqrt(v1 / n1 + v2 / n2)
    t = (m1 - m2) / se if se > 0 else 0.0
    num = (v1 / n1 + v2 / n2) ** 2
    den = ((v1 / n1) ** 2) / (n1 - 1) + ((v2 / n2) ** 2) / (n2 - 1)
    df = num / den if den > 0 else 1.0
    p = _student_t_sf_approx(t, df)
    return t, df, max(0.0, min(1.0, p))


# ---------------------------------------------------------------------------
# Effect sizes
# ---------------------------------------------------------------------------

def cohens_d(a: Sequence[float], b: Sequence[float]) -> float:
    n1, n2 = len(a), len(b)
    if n1 < 2 or n2 < 2:
        return 0.0
    m1, m2 = statistics.mean(a), statistics.mean(b)
    v1, v2 = statistics.variance(a), statistics.variance(b)
    pooled = math.sqrt(((n1 - 1) * v1 + (n2 - 1) * v2) / (n1 + n2 - 2))
    if pooled == 0:
        return 0.0
    return (m1 - m2) / pooled


def cliffs_delta(a: Sequence[float], b: Sequence[float]) -> float:
    """Non-parametric effect size in [-1, 1]."""
    if not a or not b:
        return 0.0
    n1, n2 = len(a), len(b)
    greater = sum(1 for x in a for y in b if x > y)
    less = sum(1 for x in a for y in b if x < y)
    return (greater - less) / (n1 * n2)


def interpret_cliffs(delta: float) -> str:
    d = abs(delta)
    if d < 0.147:
        return "negligible"
    if d < 0.33:
        return "small"
    if d < 0.474:
        return "medium"
    return "large"


# ---------------------------------------------------------------------------
# Sanity self-test
# ---------------------------------------------------------------------------

def _sanity() -> None:
    a = [1, 2, 3, 4, 5]
    b = [6, 7, 8, 9, 10]
    U, z, p, r = mann_whitney_u(a, b)
    assert U == 0, f"expected U=0 got {U}"
    assert p < 0.05, f"expected significant got p={p}"
    t, df, pt = welch_t_test(a, b)
    assert pt < 0.05, f"expected significant Welch p got {pt}"
    d = cohens_d(a, b)
    assert d < -2.0, f"expected large negative d, got {d}"
    cd = cliffs_delta(a, b)
    assert cd == -1.0, f"expected -1.0, got {cd}"
    m, lo, hi = bootstrap_ci(a + b, n_resamples=2000, seed=1)
    assert lo <= m <= hi
    print("stats_utils sanity OK")


if __name__ == "__main__":
    _sanity()
