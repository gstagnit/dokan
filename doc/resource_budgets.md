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
