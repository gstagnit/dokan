# Outlier trimming

Double-real channels occasionally produce a single event whose weight is
enormous — millions of times the whole cross section. The merge detects these and
removes them. This document records why that is the right thing to do, because
the question is subtle and a purely statistical reading of the data gets it
wrong.

## The phenomenon

One seed of one channel returns a value millions of times the bulk, with a
relative error near 100% — the signature of a single event carrying the entire
job. Measured in one campaign, for the worst channel:

```
seed s158:  8,557,287 fb  over 265,910 events   (median seed: 2.6 fb)
```

The sum of weights in that one job is 2.3e12, against 6.9e5 for a typical job.
**One event carries 1.14e7 times the total cross section.**

## Why these are artifacts and not physics

The decisive argument is not statistical, it is kinematic.

**All infrared singularities are confined to tcut → 0.** That is what the
technical cutoff is for: below it the double-real contribution is dropped and the
subtraction handles the limit. The physical result must be independent of the
cutoff across the range where the subtraction is doing its job — roughly 1e-10 up
to 1e-8 or 1e-7. Above that the cut starts removing genuine phase space and
independence is no longer expected.

The offending events do not live there. Binning each seed's contribution by the
tcut of its events (`tcut_diff`, remembering it is a *density* on logarithmic
bins — multiply by bin width to get a contribution):

| seed | contribution | tcut |
|---|---|---|
| s158 | 100.5% of the seed in one bin | **1e-4 – 3.2e-4** |
| s118 | 96.9% | **3.2e-3 – 1e-2** |
| typical seed | spread over 1e-10…1e-6, cancelling | — |

Those are four to eight orders of magnitude **above** the technical cutoff, in
comfortably resolved kinematics. A correctly subtracted integrand there is finite
and of order the physical scale. There is no mechanism by which a resolved,
non-singular phase-space point produces a weight 1e7 times the cross section.

What does produce exactly this is a **spurious singularity at exceptional
kinematics** — a vanishing Gram determinant, say. These are not IR singularities,
are not confined to small tcut, and cause catastrophic cancellation in the
matrix-element evaluation at otherwise unremarkable points.

## Two independent confirmations

**It reproduces across campaigns.** Two runs of charge-conjugate processes, with
different channels, different seed numbers and independent random sequences:

| run | channel | worst event | tcut bin |
|---|---|---|---|
| A | `[0,40]` | +8,557,287 | 1e-4 – 3.2e-4 |
| B | `[0,-40]` | -9,615,309 | 1e-4 – 3.2e-4 |

Same magnitude to 12%, same tcut bin (about a 3% coincidence across ~30 bins).
The seven scale variations settle it:

```
A:   8.60e6  6.38e6  1.20e7  8.61e6  6.39e6  8.48e6  1.18e7
B:  -9.62e6 -7.16e6 -1.34e7 -9.65e6 -7.18e6 -9.48e6 -1.32e7
     0.894   0.891   0.896   0.892   0.890   0.895   0.894      <- ratio
```

A **constant** ratio across all seven scales: the same matrix-element structure
evaluated at the same configuration up to one overall normalisation. Two random
samples of a smooth integrand do not do that. Two approaches to the same spurious
surface in phase space do.

**Removal is stable.** Re-merging one campaign three ways:

| | total | error |
|---|---|---|
| no removal | 218,138 fb | ± 40,754 (18.7%) |
| 1 dataset per part | 184,835 fb | ± 5,862 (3.2%) |
| all 208 flagged datasets | 184,676 fb | ± 3,078 (1.7%) |

Removing one artifact per part and removing all 208 differ by **0.09%** in the
central value while the error halves. That is what removing noise looks like:
once the worst is gone, further removals stop moving the answer. Had the flagged
datasets carried real contribution, the central value would have kept sliding.

## The trap: statistics alone cannot decide this

Worth recording, because an earlier version of this document reached the opposite
conclusion from exactly this reasoning.

Looking only at the distribution of per-seed values, the case against removal
looks strong and is wrong:

* **"Consistency with the untrimmed estimate" is vacuous.** If one dataset
  dominates, removing it shifts the mean by ~`(r_i - r_bar)/n` while contributing
  ~`(r_i - r_bar)^2/n^2` to the variance, so the shift and the error are the same
  quantity: `|shift|/sigma ~ 1` always. Measured across 24 parts: 0.04 to 1.26. A
  2-sigma test rejects almost nothing, before or after the k-scan.
* **"The flagged events carry most of the integral, so removing them changes the
  answer rather than cleaning it"** inverts the truth. A spurious event dominating
  its channel is the *symptom*. Measured share of the integral for flagged
  datasets ran to 374%.
* **"Remove outliers until the rest is Gaussian"** is not a sufficient test: it
  constrains what remains, not what was taken. It accepts a 16-dataset part whose
  single removal moves the result 372 sigma, and its required removal fraction
  grows with `n` when keyed to a fixed-alpha normality test (~2% at 50 datasets,
  ~12% at 200), which is the test gaining power, not the tails worsening.

Each of these is a correct statement about the seed-value distribution. The
distribution simply does not contain the information that settles the question.
The kinematics does.

## Settings

```
trim_threshold      8      robust-z above which a dataset is flagged (0 disables)
trim_max_fraction   0.05   safety valve: at most max(1, fraction * ndat) removed
```

`trim_max_fraction` is a **safety valve, not a budget**, and the floor of one
matters. Written as a bare fraction, the first removal required
`ndat >= 1/fraction` — 143 datasets at the old default of 0.007 — so trimming
switched on when a part had accumulated enough statistics rather than when the
data called for it. A campaign then ran with removal silently active for its
large parts and inert for the rest, split across that line, with the affected
parts each losing exactly one dataset regardless of how many were flagged.

The z-score itself is two-sided, computed against the sample's robust scale with
a separate MAD below and above the median (the distribution is skewed, so a
one-sided scale would bias rejection toward the longer tail) and weighted by
`sqrt(neval/<neval>)` so a better-sampled job is held to a tighter tolerance.

Trimmed datasets are pooled into a trailing slot which is marked `INVALID`: they
are discarded, not down-weighted.

## Diagnostics

`MergePart` reports per part whenever the detector finds anything:

```
MergePart[<part>]::run:  outliers in 1845/2044 bins (90%); 4% of dataset-slots
    removed (32295/860524); worst holds 62% of a determined bin (ptl_1j_IFN_osss);
    cap reached in 31/2044 bins (2%); no k-scan plateau in 239/2044 bins (12%)
```

Every figure carries its denominator. Without one these read as a catastrophe and
are not: a double-real channel flags something in nearly every bin while removing
a few percent of the data.

* **`X% of dataset-slots removed`** is the number that matters — 3-4% on real
  campaigns.
* **`cap reached in N/M bins`** is the one genuine warning: a bin held more
  outliers than `trim_max_fraction` allows removing, so the loop stopped on the
  valve rather than the threshold and contamination remains.
* **`no k-scan plateau`** paired with a large relative error means the seed sample
  cannot resolve the tail. Alone it is not necessarily trouble.
* **`worst holds X% of a determined bin`** is reported only where the bin is at
  least a 2-sigma measurement. The denominator is a sum with cancellations, so a
  bin consistent with zero sends the ratio to absurd values — 310,000% was
  observed before this guard — that say nothing about the data.

## Checking that the parameters are right

The test is insensitivity: vary `trim_threshold` over an order of magnitude and
confirm the answer moves by less than its error. Measured on two campaigns, with
`trim_max_fraction = 0.05`:

| `trim_threshold` | campaign A (fb) | campaign B (fb) |
|---|---|---|
| none | 201,817 +/- 27,128 | 179,301 +/- 8,029 |
| 8 | 182,332 +/- 2,474 | 179,262 +/- 2,080 |
| 15 | 186,418 +/- 2,380 | 179,197 +/- 2,373 |
| 30 | 185,198 +/- 3,320 | 181,525 +/- 3,032 |
| 100 | 187,290 +/- 3,596 | 177,786 +/- 2,177 |

Every trimmed row agrees within its error; only "no trimming" stands apart, and
its error is an order of magnitude larger. The k-scan is equally stable —
`k_scan_nsteps` 2 to 5 and `k_scan_maxdev_steps` 0.2 to 0.8 span 179,468 to
184,964 fb against errors of 1,600-3,300.

Note that the robust-z distribution is a **continuum**, not two separated
populations — a representative bin runs 109, 35, 19, 17, 16, 15, 15, 14, 12, 11,
10, 10, 9.8, 9.1, 8.1, 7.9, ... so there is no gap to put the threshold in. That
is precisely why the insensitivity check matters more than the choice of 8.

If a channel is flagged persistently and heavily, the artifacts are worth chasing
at the source rather than only removing here: the events are reproducible (the
runs are deterministic, and the seed is named in the diagnostics), so the
offending configuration can be examined directly.
