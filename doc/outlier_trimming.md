# Outlier trimming, and why it is off

Double-real channels produce occasional events whose weight is orders of
magnitude above the bulk. A single seed can return a value millions of times the
channel's size, with a relative error near 100% — one event carrying the whole
job. The obvious response is to discard such seeds. This document records why
dokan does not, and what it does instead.

## The estimator is already a bias/variance ladder

`MergeObs` does not simply average the per-seed results. It runs a **k-scan**:

| rung | estimator | property |
|---|---|---|
| step 0 | inverse-variance weighted mean over all datasets | minimum variance, **biased** |
| … | `merge_pair()` pools two pseudo-datasets and re-combines | intermediate |
| last | one pseudo-dataset: the fully pooled event-level mean | **unbiased**, maximum variance |

The bias at step 0 is not a subtlety, it is the central difficulty of heavy-tailed
integrands: a seed that happens to sample a large-weight event gets both a large
`|result|` **and** a large variance, so the inverse-variance weight demotes it in
exact proportion to how much it would have moved the answer. Pooling has no such
correlation and is unbiased.

The scan climbs the ladder and stops at the first *plateau* — when successive
rungs agree within `k_scan_maxdev_steps` of their combined error. **That plateau
test is the bias control**: it certifies that the low-variance estimate and the
progressively unbiased ones have converged.

## Why trimming before the k-scan breaks it

Trimming removes the disputed datasets from the sample the ladder is built on.
Both ends move: the pooled end no longer contains the events either. The two ends
then agree trivially, a plateau is declared early, and what has been certified is
"these agree on a sample the disputed events were already removed from".

Trimmed datasets are pooled into a trailing slot which is then marked `INVALID` —
they are **discarded**, not down-weighted. (A comment in `_core.py` long claimed
they "will eventually be suppressed in the weighted average by the large error";
that describes a different, safer design than the code implements.)

Suppressing rather than discarding — keeping the pooled outlier slot `ACTIVE` so
it rejoins as the ladder climbs — is structurally better and sometimes reproduces
the untrimmed answer exactly. It is still not sufficient: the plateau can fire
before the outlier slot rejoins.

## Trimming cannot be validated from the data

The natural safety test is "trim only if the result stays consistent with the
untrimmed one". **That test is vacuous**, and not for want of statistics.

If one dataset dominates a sample of `n`, removing it shifts the mean by

```
Delta ~ (r_i - r_bar) / n
```

while its own contribution to the variance is `~ (r_i - r_bar)^2 / n^2`, so

```
sigma >~ |r_i - r_bar| / n ~ |Delta|
```

**The shift and the error are the same quantity.** An outlier inflates the
untrimmed error in exact proportion to how much removing it would move the
answer, so the untrimmed estimate can never reject the trim that removes it. The
prediction is `|Delta| / sigma ~ 1`, and that is what is measured: across 24
affected parts of one campaign the ratio ran 0.04 to 1.26, against both the
pooled and the plateau anchor. A 2-sigma test rejected **1** of 17 cases in which
the discarded seed carried more than 40% of its part's integral.

Applying the test after the k-scan rather than before does not help: the anchor's
error is inflated by the very outlier in question.

## What does discriminate: the share of the integral

The one quantity that separates a spurious outlier from a real contribution is
how much of the integral it carries:

```
share = |sum over flagged datasets of sumf| / |sum over all datasets of sumf|
```

A numerical artefact contributes almost nothing to the integral. A large-weight
event that is physically real carries a sizeable fraction of it — and may be
cancelling against opposite-sign events of similar size, which is why `share` can
exceed 1.

Measured across two campaigns, for every part a sparsity+Gaussianity gate would
have accepted:

| candidates' share of the integral | parts | error gain from removing them |
|---|---|---|
| < 5% | 4 of 13 | x0.88 – x0.99, i.e. nothing |
| 15 – 70% | 3 | meaningful |
| 70 – 374% | 6 | large |

**Trimming is safe only where it does not matter, and matters only where it is
not safe.**

### Why a Gaussianity criterion is not enough

"Remove the outliers if the rest then fits a Gaussian" is the right instinct but
an insufficient test: it constrains what remains, not what was taken away. A part
with 16 datasets, 15 of them in `[-672, +1785]` and one at `-1.09e6`, passes it —
one candidate is sparse, and the remaining 15 are clean (scale ratio 1.06).
Removing that one dataset takes the answer from `-85,239 +/- 85,097` to
`+179 +/- 230`: the error falls by a factor of 370 and the central value moves by
372 sigma of the new error.

Note also that a fixed-alpha normality test is the wrong instrument regardless,
because its threshold is about detectability: the fraction that must be removed
to pass Anderson-Darling *grows* with the number of seeds (measured: ~2% at 50
datasets, ~12% at 200 for the same contribution). An effect size such as
`classical sigma / robust sigma` is about twice as stable, but still not fully.

## What dokan does instead

`merge.trim_max_fraction` defaults to **0**: the detector runs and reports,
nothing is removed. `merge.trim_threshold` (default 8) still sets the robust-z
above which a dataset is flagged; setting *it* to 0 switches detection off too.

This also removes an accident. Because the cap was a pure fraction, the first
removal required `ndat >= 1/trim_max_fraction` — 143 datasets at the old 0.007.
Trimming was therefore off for small parts and on for large ones, a switch on
statistics rather than on data quality, and campaigns were internally split
across that line.

`MergePart` logs a per-part summary whenever the detector finds anything:

```
MergePart[<part>]::run:  outliers flagged in 3 bin(s), up to 5 per bin,
    worst carrying 97% of the integral (cross); no k-scan plateau in 1 bin(s):
    more seeds needed
```

Read it as follows.

* **`no k-scan plateau`** is the signal worth acting on. The ladder ran to the
  fully pooled estimate without its ends ever agreeing, which means the seed
  sample cannot resolve the tail. Paired with a large relative error it says:
  add seeds. On its own it does not necessarily indicate trouble — a part can
  reach the pooled end and still be perfectly precise.
* **`carrying X% of the integral`** is the reason nothing was removed. Near zero,
  the flagged datasets are noise; of order one, they are the result.

The estimator does recover on its own. One part went from
`-230,957 +/- 235,129` at 48 datasets to `-1,521 +/- 2,020` at 94, untrimmed:
the k-scan found its plateau once the seeds arrived.

## Re-enabling removal

Set `trim_max_fraction` to a non-zero value in the runcard's `[Options]`
(`trim = 8 0.05`) or in `config.json`. Before doing so, check the reported share
for the parts it would affect: if it is not small, the removal is changing the
answer rather than cleaning it, and the resulting error bar will not cover the
difference.

Changing either merge setting is part of the `MergeObs` freshness identity, so
existing results re-merge automatically; `nnlojet-run finalize RUN --reset`
forces a full rebuild.
