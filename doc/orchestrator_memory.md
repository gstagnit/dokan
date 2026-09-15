# The orchestrator's memory growth (unresolved)

The `submit` process grows at roughly **0.9 GB/hour**, linearly, for as long as it
runs. This is the single largest operational hazard in a long campaign and it is
**not yet diagnosed**. This document records what is measured, what has been
ruled out, what was tried and failed, and how to actually find it — so the next
attempt does not repeat this one.

## What it costs

Nothing until the machine's limit is reached, and then everything at once. Luigi
forks a process per task attempt, so the parent's heap is shared (copy-on-write)
into every fork. On a login node with a 34.3 GB per-user cgroup, two campaigns
running together reached:

```
Tasks:  198  (190 orchestrator processes)
Memory: 27.4G  (max: 34.3G  peak: 31.0G)
```

At that point the node reported `some_avg300 = 57.86%` memory pressure, the
kernel began OOM-killing, and the processes that were *tracking batch jobs* were
among the casualties. That is the expensive part: the jobs themselves keep
running on the batch system and complete normally, but nothing marks them done,
so the dispatcher sees its in-flight count pinned above the concurrency limit and
refuses to dispatch forever. See "the orphaned-job wedge" below.

## The measurements

Two independent campaigns, before any fix:

| uptime | parent RSS |
|---|---|
| 6 h 44 m | 5.41 GB |
| 10 h 36 m | 8.40 GB |

0.81 and 0.79 GB/h — the same rate, consistent with linear growth from zero. The
fork population confirms it independently: a fork inherits the parent's heap, so
the shared footprint of each child dates its creation (2.2 GB for one made ~3 h
in, 4.7 GB for one made 26 minutes ago).

Sampled directly on a third run, 90 s apart:

```
0.804 -> 0.825 -> 0.847 GB      = 0.86 GB/h
```

Composition at that moment:

```
Rss            0.854 GB
Anonymous      0.835 GB      <- heap, not file cache
Shared_Dirty   0.810 GB      <- COW-shared into ~100 forks
Private_Dirty  0.025 GB
Pss            0.091 GB
```

So it is anonymous heap in the parent. The parent's *proportional* share is small
because the forks share those pages; the cgroup total is roughly
`parent heap + (forks x private)`.

## Ruled out

* **Not file-backed / page cache.** `Anonymous` is 98% of `Rss`.
* **Not per-task retention.** This was the hypothesis, and it was wrong — see
  below.
* **Not log accumulation.** Growth continues with the log database static
  (14626 rows unchanged over a minute) and seven log records in ten minutes.
* **Not a long-lived SQLAlchemy identity map in the monitor.** `Monitor.run()`
  opens a fresh session per refresh.
* **Not merge or HDF5 activity.** It continues at full rate while the run is
  essentially idle.

That last point is the strongest constraint and the most useful one: **the
orchestrator grows at ~250 kB/s while doing nothing.** Whatever it is, it is
driven by a polling loop, not by work.

## The attempt that failed

Luigi retains every task it schedules — `Worker._scheduled_tasks`,
`Worker._add_task_history`, and the scheduler's own table, whose `update_status`
marks a task removable only once it has no stakeholders, which never happens
while a single worker stays alive (so `prune_on_get_work=True`, which dokan sets,
finds nothing it may remove). On top of that, `Worker._add_task` *serialises* each
task's parameters and the scheduler keeps that dict for the task's lifetime, and
`json.dumps` returns a fresh string every call — so every scheduled task pinned
its own copy of the ~21 kB run configuration.

That is real, and memoising `SharedDictParameter.serialize()` fixed it:

| | retained per task | distinct config strings (400 tasks) |
|---|---|---|
| before | 26.9 kB | 400 |
| after | 0.6 kB | 1 |

**And production growth did not change at all**: 0.86-0.99 GB/h afterwards,
against 0.80 GB/h before.

The arithmetic says why the hypothesis was never viable, and it is worth
internalising because it could have been checked *first*: at 26.9 kB per task,
0.9 GB/h needs about 9 task instances per second, which is plausible; at 0.6 kB
it would need about 420 per second, which is not. A fix that cuts per-task cost
by 45x while the growth rate does not move proves the growth was never
proportional to tasks.

The lesson: a micro-benchmark measuring "how much does X retain" cannot tell you
whether X is what is growing. That needs the rate, in production, before and
after.

The memoisation is kept regardless — it removes a genuine per-instance cost and
is ~60x faster per call — but it is not this.

## How to actually find it

Stop inferring from `/proc`. `smaps` can say *what kind* of memory is growing and
has now said all it can. The next step is from inside the process:

1. Add an opt-in debug hook to `submit` that, every N minutes, logs
   `tracemalloc.take_snapshot().compare_to(previous, 'lineno')[:20]` and
   `len(gc.get_objects())` broken down by `type(...).__name__`. Both are cheap
   enough to leave running for an hour.
2. Run a single campaign with it for two hours. A linear 0.9 GB/h leak is ~1.8 GB
   of growth — trivially visible in a tracemalloc diff.
3. The prime suspects, given that it grows while idle, are the loops that run
   regardless of work: `DBDispatch._poll_dispatch_signals_until`, the `Monitor`
   refresh, and the `DBRunner` executor polling. Look for an accumulator in one
   of those — a list appended per poll, a cached session, a rich renderable
   retained by the `Live` display.

Until then, the mitigation below is the answer.

## Mitigation (current practice)

* **Restart the orchestrator periodically** — every 8-12 hours on a 34 GB limit.
  A restart is cheap and designed for: the database holds the state, in-flight
  jobs are re-attached by `DBResurrect`, and the heap starts from zero. This is
  hygiene on a long campaign, not a failure.
* **Restart before a final merge.** That is the fork-heavy phase and therefore
  where the accumulated heap does the most damage.
* **Keep `jobs_max_concurrent` sized to the forks, not the batch system.** The
  fork count is roughly `jobs_max_concurrent / jobs_batch_size`. Two campaigns at
  2000 concurrent with a batch size of 23 gave ~174 forks, which matched the 190
  processes observed at the OOM. Halving the concurrency halves the forks.

## The orphaned-job wedge

Worth stating separately because it is the failure this leak actually produces,
and it is independently fixable.

When a tracking fork is killed, its jobs stay `RUNNING` in the database forever.
The dispatcher's throttle then compares a phantom in-flight count against
`jobs_max_concurrent`:

```
DBDispatch[0,51]::repopulate:  2212/2587 in-flight v.s. 2000 max -> throttled
```

and never dispatches again, while nothing can ever complete the phantoms. Two
campaigns sat in exactly this state for nine hours with one real batch job left
between them.

`nnlojet-run doctor RUN --scan-dir` is the repair: it rescans the execution
directories and reconciles against what is on disk. On the two campaigns above it
cleared 2662 and 2587 phantom jobs and recovered 1981 completed jobs each — work
that had finished on the batch system and would otherwise have been thrown away.

A live run cannot currently notice this itself; `DBResurrect` only runs at
startup. A periodic reconciliation — or simply treating "in-flight at the cap
with no state change for several retry intervals" as a trigger — would turn a
nine-hour wedge into a self-healing pause.
