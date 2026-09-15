# Resource budgets: what bounds a run, and what it costs

How dokan decides to keep going or stop, how long a single job is allowed to
take, and where each of those numbers is enforced. Both sections describe
mistakes that are easy to make because the parameter names sound like they mean
something they do not.

## Bounding the campaign

### `jobs_max_total` bounds the wrong quantity

A run is bounded by two things: an accuracy target, and a resource cap. Until
recently the only resource cap was `jobs_max_total`, a *count* of production
jobs — and a count is not what a run spends. What it spends is time. The two
agree only if every job fills its allotted slot, and they do not: a job stops
when it has integrated the events it was given.

The failure this produces is quiet and easy to miss. A run whose jobs typically
finish in a third of `job_max_runtime` exhausts its job count having consumed a
third of the compute it was implicitly granted. It then stops, short of its
accuracy target, reporting only "budget exhausted". Nothing says that two thirds
of the intended CPU was never used.

A runtime bound was in fact already present, inside `_remainders`:

```python
t_rem = jobs_max_total * job_max_runtime - t_alloc
```

but only as a *derived* quantity — not settable, not reported, and in practice
never the binding constraint, since the count runs out first whenever jobs come
in under their slot.

### `jobs_max_total_runtime`

`run.jobs_max_total_runtime` (`--jobs-max-total-runtime`, accepting `30000h`,
`2d`, …) budgets the whole computation directly, and `jobs_max_total <= 0` lifts
the job cap so the runtime can be the only limit.

**The two caps are measured against different things**, because they answer
different questions:

| cap | scope | why |
|---|---|---|
| `jobs_max_total` | this submission's production jobs | it is a queue-load guard — the batch system cares how many jobs exist — and a resubmit is meant to be able to dispatch again |
| `jobs_max_total_runtime` (explicit) | **every** job of **every** submission, warmup included | it bounds the campaign |
| runtime cap (derived, legacy) | this submission's production jobs | unchanged, so existing configurations behave exactly as before |

`budget()` returns whether the runtime cap was set explicitly and `_remainders`
pairs each cap with its own accounting. Mixing them would be a category error:
an explicit campaign budget paired with per-submission spend would reset on
resubmit and bound nothing, while the derived cap paired with campaign-wide
spend would let a resubmit inherit spend against a cap that had just reset.

Warmup counts toward an explicit budget, and this is not a detail — warmup can
be a large fraction of everything a run consumes (comfortably 40% in a
real campaign). A "total runtime" that ignored it would not bound the total.

### Lifting both caps is refused

`submit` exits if `jobs_max_total <= 0` *and* `jobs_max_total_runtime <= 0`,
because the run would then have no termination condition. The accuracy target is
not one. An error estimate is itself a statistical quantity and can *rise* as
statistics arrive — a run has been observed going 20.4% → 23.0% → 24.7% → 24.8%
over successive merges as late large-weight events revised it upward. A target
can recede faster than a run approaches it, and an unbounded run would then
never stop.

### Knowing before the budget runs out

dokan estimates the runtime still needed to reach the target — `T_target`, from
the Euler-Lagrange allocation in `_distribute_time` — and has always reported
it:

```
reached rel. acc. 24.8% on cross_hist (requested: 10.0%)
still require about <T> of runtime to reach desired target accuracy (approx. N jobs)
```

but only from `MergeFinal`, i.e. **once the run is over**. That is an epitaph,
not a forecast: by the time it prints, the budget is spent and the decision it
would have informed is no longer available.

The same numbers now accompany the periodic cross-section update:

```
cross = ... fb
current "cross_hist" error: 24.8% (requested: 10.0%)
budget: 42d 19h / 125d runtime  ·  3000 / 3000 jobs
still need ~5d but only 2d 3h left -> target not reachable within budget
```

This costs nothing: `MergeAll` already called `_distribute_time` to compute the
optimisation target, so `T_target` was already in hand and simply thrown away.
The consumption figures are two `SUM`/`COUNT` queries.

`T_target` from a single pass is a first-order estimate — `MergeFinal` iterates
it to convergence — hence the `~`.

### Why the runtime-to-target estimate was wrong

The estimate is a Euler-Lagrange optimum: to reach an absolute accuracy `A`, the
total runtime wanted is

```
T_needed = (sum_i sigma_i sqrt(T_i))^2 / A^2  -  sum_i T_i
```

Everything turns on **which parts belong in those sums**, and that was the bug.

`_distribute_time` first runs an exclusion loop, dropping parts whose optimal
allocation comes out negative — parts already holding more time than the optimum
would give them. That set is correct for *allocating* a budget, and it depends on
the budget. But the reporting paths have no budget to pass: the estimate is what
they are asking for, so they pass a token one-second `total_t` purely to avoid a
division by zero. At one second **every** part already holds more than its share,

```python
t_opt = (i_err_sqrtt / accum_err_sqrtt) * (total_t + accum_t) - i_t
```

goes negative almost everywhere, and the loop excludes nearly everything —
measured on real campaigns, **1 of 180 parts survives**. The sums then describe a
single part, and the estimate collapses by orders of magnitude.

The first attempt at a fix guarded on `T_target <= 0` and substituted plain
`1/sqrt(T)` scaling. That caught the case where the collapse happened to land at
or below zero, and missed the general one: the collapse lands wherever the
surviving subset puts it, and a small **positive** answer sails straight through
the guard. A campaign far from its target reported **3.3 kh** of remaining
runtime when the true figure was above 86 kh.

### The bracket

The estimate is now clamped between two bounds that hold no matter what the
exclusion set did:

| bound | assumption | why it bounds |
|---|---|---|
| lower | the same E-L formula over **every** part | the *unconstrained* optimum, free to take time away from over-resourced parts as reality is not — so it can only be optimistic |
| upper | no reallocation at all; every part scaled together, total error falling as `1/sqrt(T)` | any allocation does at least this well |

```python
t_el     = (accum_err_sqrtt / A) ** 2 - accum_t          # the kept set
t_el_all = (accum_err_sqrtt_all / A) ** 2 - accum_t_all  # every part
t_flat   = accum_t_all * ((tot_error / A) ** 2 - 1.0)
T_target = min(max(t_el, t_el_all), t_flat)
```

Neither bound depends on `total_t`, so a token probe can no longer produce a
nonsense answer. Replaying two campaigns' part tables through the exclusion loop:

| probe budget | parts kept | E-L on kept set | reported before | reported now |
|---|---|---|---|---|
| 1 s (what the reports pass) | 1 / 180 | 48 kh | 48 kh | **86 kh** |
| 500 h (too small) | 2 / 180 | 59 kh | 59 kh | **86 kh** |
| 86 kh (right scale) | 26 / 180 | 86.7 kh | 86.7 kh | **86.7 kh** |

The last row is the one that matters for not breaking anything else. When a
caller iterates towards a realistic budget — as `MergeFinal`'s refinement loop
does, re-solving `while T_target / prev_T_target > 1.3` — the E-L answer already
sits inside the bracket and passes through unchanged, so the sharper constrained
number still wins. The clamp bites only when the accumulators cannot be trusted.
Note also that 86 kh in gives 86.7 kh out: at the right scale the estimate is
close to a fixed point, so refinement converges in about one step.

For a campaign that has **reached** its target both bounds go negative and the
estimate is zero, as before.

One residual case needs the old fallback. The lower bound can itself be
non-positive while the target is still out of reach: unconstrained, shifting time
between parts can close the gap with no new time at all. Reality cannot, so the
estimate falls back to `t_flat` there — pessimistic, but the right kind of wrong
for a "you still need about X" statement.

### The estimate is only as good as the error it chases

`A = target_rel_acc * tot_result`, and `tot_result` is a sum over parts with
large cancellations. A campaign whose total sits near zero because one part is
badly determined has a target that is *itself* unstable, and no runtime estimate
against it means much. Read the estimate together with the per-part errors
(`select name, result, error from part order by abs(error) desc`) — if one part
carries almost all of `tot_error`, that part, not the budget, is the thing to
deal with.

### The accuracy being reported is not the cross-section error

`run.opt_target` selects what the run optimises towards and reports. The default
`cross_hist` is

```python
rel_cross_err = sqrt(rel_cross_err * max_rel_hist_err)
```

a geometric mean of the cross section's own relative error and the **worst**
relative error over the observables. `max_rel_hist_err` is taken over each
observable's *integral*, and that is where it used to go wrong.

A differential distribution with large bin-to-bin cancellations can integrate to
something consistent with zero while keeping a perfectly finite error. Its
relative error is then meaningless and unboundedly large. One measured case:

```
abs_yj1_1j_GHS_osss   integral  -0.13 +/- 947.7   ->  |e/r| = 7160
abs_yj1_2j_GHS_osss   integral   -708 +/-   7.1   ->  |e/r| = 0.010
```

The old guard was `if r != 0.0`, which catches exact zeros and misses precisely
this. That single observable set the maximum for its part, and through it the
part's whole optimisation error: 70,415 against a cross section of
`-947.7 +/- 730.8`.

The damage was not cosmetic. `_distribute_jobs` allocates by `Part.error`, so such
a part accounts for ~100% of the quadrature error sum and soaks up the entire
remaining budget chasing a relative error that *cannot* improve — the denominator
is zero by cancellation, not by lack of statistics. The run then reports an
accuracy and a `T_target` that describe nothing real.

An observable must now be a measurement before its relative error may set the
maximum:

```python
significant = [(r, e) for r, e in cross_list if r != 0.0 and abs(r) > 2.0 * e]
max_rel_hist_err = max(abs(e / r) for r, e in significant) if significant else rel_cross_err
```

When nothing is significant the part is optimised on its cross section alone,
which is the honest degradation — the histograms genuinely say nothing about how
well it is determined. (The previous code appended a synthetic `(1.0, 1e-9)`
entry to keep the maximum non-empty; with the filter in place that would instead
make such a part look perfectly determined, so it is gone.)

Measured across two campaigns of 180 parts each:

| | reported accuracy before | after | actual cross-section error |
|---|---|---|---|
| campaign A | 39.3% | **0.71%** | 1.13% |
| campaign B | 2.6% | **1.09%** | 1.41% |

with a median of 16 and 12 significant observables per part respectively, and 36
parts in each falling back to the cross error — the near-empty channels that are
excluded from the budget anyway.

Note that `cross_hist` can legitimately come out *below* the cross-section error:
it is a geometric mean, so histograms that are relatively better determined pull
it down. That is the existing design of the target, not a side effect of this
guard.

### Units

CPU budgets are reported in hours, or kilo-hours once large (`format_cpu_time`),
*not* through `format_time_interval`. The latter renders a wall-clock duration as
d/h/m/s, which is the wrong unit for a resource: a 30000-hour budget shown as
"1250d" reads as elapsed time, when it is an amount of compute a few thousand
cores get through in an afternoon.

## Bounding a single job

### `job_max_runtime` is an integration budget, not a wall-clock limit

`job_max_runtime` is what dokan sizes jobs *against*: `assess_warmup` and
`size_preproduction` choose `ncall` so that a job's integration fills it.
NNLOJET itself is given no time limit — only `ncall` and `niter` — so this
estimate is dokan's *only* control over how long a job runs, and overshoot is
intrinsic.

What the batch system enforces is **wall** time, which additionally covers
NNLOJET's startup, the PDF initialisation, and input/output transfer. Handing
it the bare integration budget therefore leaves a job that used its whole budget
no room at all:

```
_dbrunner.py   policy_settings["max_runtime"] = job_max_runtime
lxplus.template   +MaxRuntime = ${max_runtime}       # HTCondor
slurm.template    #SBATCH --time=${max_runtime}      # Slurm
```

The result is a job killed outright, with everything it produced discarded:

```
009 (...) Job was aborted.
    Job removed by SYSTEM_PERIODIC_REMOVE due to wall time exceeded allowed max.
```

No `.dat`, no `.khd`, no `.out`, `elapsed_time = 0` — a full slot of CPU spent
for nothing. Observed at **~1.5% of warmup jobs**, concentrated in the most
expensive contributions (`RRa`, `RRb`), in two independent campaigns.

This is not a rare edge. The sizing *aims* at the cap, so a healthy run puts its
jobs right underneath it: in one campaign the longest successful warmup job ran
59.5 min against a 60 min limit, with 15 jobs inside the last 10%.

Nor does the existing `tau_buf` protect against it. That buffer,

```python
tau_buf = min(10 * ires["tau_err"], 0.5 * ires["tau"])
```

scales with the *error on the mean* per-event time, which shrinks as statistics
accumulate — while the spread between seeds of the same step does not. The
buffer is therefore smallest exactly where the per-event time is best measured,
which is where jobs graze the limit.

### `job_max_runtime_margin`

The batch system is now asked for more wall time than dokan intends to use:

```python
max_runtime = max(job_max_runtime * (1 + job_max_runtime_margin),
                  job_max_runtime + _WALLTIME_FLOOR)
```

`run.job_max_runtime_margin` defaults to `0.15`. The floor (`_WALLTIME_FLOOR`,
300 s) matters for short budgets, where a percentage alone would not cover a
startup cost that is roughly fixed however long the job then integrates for.

| `job_max_runtime` | requested wall time | headroom |
|---|---|---|
| 10 min | 15 min | 5 min (floor) |
| 30 min | 35 min | 5 min (floor) |
| 60 min | 69 min | 9 min |
| 8 h | 9.2 h | 72 min |

Raising the margin is cheap insurance; a killed job costs a whole slot and
produces nothing. Lower it only if the site penalises longer requested runtimes
in scheduling.

### That ceiling is not what every job should ask for

Only the expensive contributions are actually sized to fill `job_max_runtime`.
The Euler-Lagrange allocation in `_distribute_time` gives a cheap, well-converged
part very little time, and `ntot_job` follows the *allocated* time rather than
the cap — so those jobs finish in a small fraction of it. Median elapsed as a
fraction of the cap, over one campaign's production jobs:

| LO | V | VV | R | RV | RR |
|---|---|---|---|---|---|
| 1.5% | 1.6% | 3.1% | 10% | 32% | 39% |

Requesting the ceiling for all of them made a job that runs for under a minute
look, to the scheduler, exactly like one that runs for an hour: matched against
fewer slots and queued behind nothing it resembles. It also coupled two
decisions that should be independent — raising `job_max_runtime`, which is worth
doing because a longer job gives a better-behaved per-job estimate for the
contributions with long weight tails, would drag the request of every trivial
job up with it.

So the ceiling stays a ceiling, and each job asks for what it is expected to
need:

```python
expected = ncall * niter * tau            # tau from this part's recent jobs
requested = min(wall_cap, expected * job_runtime_safety_factor + _WALLTIME_FLOOR)
```

`tau` (`DBRunner._recent_tau`) is size-weighted — total time over total events
across the last `_TAU_SAMPLE` completed jobs, not a mean of per-job ratios —
because it is used to predict a large job, so large jobs should dominate it.

**It is keyed on the individual part *and* the mode, and neither may be
relaxed.** Channels inside one contribution differ in cost by orders of
magnitude, and a part integrates far more slowly in production than in warmup.
An early version of this analysis pooled channels by contribution and
mispredicted by up to a factor of 78 — worth remembering before "simplifying"
the key.

The safety factor absorbs the seed-to-seed spread within a single step, which is
not small: measured against the batch median, 1.26x typical, 1.76x at p90, with
a tail to ~7x. Replaying two campaigns' completed jobs through the rule:

| safety | jobs killed that survive today | slot-time requested |
|---|---|---|
| 2 | 7 of 5623 | 66% |
| 3 | 1 of 5623 | 77% |
| **4** | **0 of 5623** | **80%** |
| 6 | 0 of 5623 | 84% |

`run.job_runtime_safety_factor` therefore defaults to **4**. Setting it to 0
disables the estimate and restores the "always request the ceiling" behaviour.

The effect on what gets asked for, per contribution (same campaign, 60 min
budget, all of them requesting 69 min before):

| LO | V | VV | R | RV | RR |
|---|---|---|---|---|---|
| 5.5 min | 7.3 min | 12.6 min | 12.1 min | 69 min | 69 min |

which moves 29% of jobs out of the longest scheduling band into faster ones,
while the contributions that genuinely need the full budget keep it.

A part with no completed job in that mode yet has no `tau`, and falls back to the
ceiling — the conservative direction, since over-requesting only costs
scheduling priority whereas under-requesting kills the job.

Note that a killed job is *wasteful*, not *incorrect*: dokan marks it FAILED,
excludes it from the budget, adapts the warmup step on the seeds that did come
back, and carries on — which is why this produces no ERROR or WARN entries and
can run for a whole campaign unnoticed. Look for it in the job table, not the log:

```bash
python3 -c "
import sqlite3
d = sqlite3.connect('file:db.sqlite?mode=ro', uri=True)
print(list(d.execute('select count(*) from job where status=-1')))"
```

and confirm the cause in the batch system's own log for the affected directory
(`raw/<mode>/<part>/s<seeds>/job.log` for HTCondor).
