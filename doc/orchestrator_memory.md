# The orchestrator's memory growth (diagnosed)

The `submit` process grew at roughly **0.9 GB/hour**, linearly, for as long as it
ran. This was the single largest operational hazard in a long campaign. It is now
diagnosed — the cause is in Luigi's worker, not in dokan's tasks — and bounded in
`dokan/scheduler.py`; see "The cause" below. The measurements, the ruled-out
hypotheses and the failed attempt are kept because they are what pointed at it,
and because the same reasoning applies to the next one.

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

## The cause

`luigi.worker.Worker._get_work_response_history`. Every call to `Worker._get_work`
ends with

```python
self._get_work_response_history.append({"task_id": task_id, "running_tasks": running_tasks})
```

where `running_tasks` is what `Scheduler.count_pending` builds for the reply: one
fresh dict per task currently RUNNING — `task_id`, the worker string, `host`,
`username`, `pid`, `workers`. The list is never drained. Its only reader is
`luigi.execution_summary._get_external_workers`, at the end of the run, and only to
name tasks that *other* workers ran — which a single local worker never has.

The worker calls `_get_work` continuously. Once it has running tasks and nothing
left to schedule, its main loop is: ask the scheduler for work (nothing), wait
`wait_interval` on the result queue (nothing), repeat. dokan's factory set
`wait_interval = 0.1`, so the poll ran about nine times a second, and the submit
orchestrator lives in exactly that state — it *is* a parent of ~100 tracking forks
with nothing to schedule between batch-job completions.

That matches every constraint above:

* it grows while idle, at a constant rate, because the rate is set by the poll
  interval and the fork count, not by work;
* it is anonymous heap in the parent, shared into every fork made afterwards;
* it did not move when the per-task retention was cut 45x, because it is not
  per task, it is per poll;
* it did not show in the log database, the sessions, or the merges.

Measured offline with Luigi 3.8.1, a local scheduler and one worker, by calling
`get_work` the way the loop does and tracing the heap:

| running tasks | retained per poll | at 9 polls/s |
|---|---|---|
| 100 | 27.6 kB | 0.89 GB/h |
| 200 | 54.9 kB | 1.78 GB/h |

The production campaigns above had ~100 forks each and grew at 0.80–0.99 GB/h.
Re-sampled on two fresh campaigns while this was being written, 111 s apart,
forty minutes in:

| forks | parent RSS growth | rate |
|---|---|---|
| 95 | 28.1 MB | 0.91 GB/h |
| 130 | 37.5 MB | 1.22 GB/h |

The rate scales with the fork count, as it must if it is per poll x per running
task. (By then the parent's heap was already 90% private rather than shared:
the forks predate most of it, and what they do share the parent keeps
rewriting.)

Why it was not found sooner: the arithmetic in "The attempt that failed" says a
per-poll accumulator was the only kind that could fit, and `worker.py` has exactly
one `list.append` on the poll path. The same reasoning that ruled out per-task
retention would have found it — the lesson stands.

## The fix

`WorkerSchedulerFactory.create_worker` replaces the list with
`collections.deque(maxlen=100)`, exactly as it already did for
`_add_task_history`. `execution_summary` only iterates the structure, so nothing
else changes. The retained heap is now flat at roughly `100 x (per-poll size)`,
under 3 MB for 100 forks.

The submit orchestrator also now runs the worker at Luigi's own defaults,
`wait_interval = 1.0` and `ping_interval = 1.0`, instead of the factory's 0.1 s.
Each idle round prunes and re-sorts the whole task table (which, as noted below,
Luigi never shrinks while the worker lives) and `waitpid`s every child; at ten
rounds a second that was the parent's ~5% of a login-node core for no benefit.
A result that arrives on the queue is handled immediately regardless of the
interval, and the forks poll HTCondor once per `htcondor_poll_time` (100 s), so
the extra 0.9 s of worst-case scheduling latency is invisible. The local-execution
and finalize builds keep 0.1 s, where the tasks are short and the latency matters.

**Status.** The offline measurement reproduces the production rate to within the
fork count, and the bounded history is verified through Luigi's real `_get_work`
path (1000 polls, history length 100, heap flat). The production confirmation —
a campaign started on the fixed build holding a flat RSS over several hours — is
still to be recorded here. Note that the installed `dokan-venv` is a plain (not
editable) install: the fix reaches a campaign only after `pip install` into the
venv and a restart of that campaign's `submit`.

## What remains

The fixed orchestrator is small and flat, but the forks are not free, and on a
shared login node they are now the dominant cost:

* **One process per in-flight batch of jobs**, ~15–45 MB private each after
  copy-on-write drift, plus its share of the parent. ~100 forks is ~2–4 GB of
  real footprint per campaign. The lever is the batch size: the fork count is
  roughly `jobs_max_concurrent / jobs_batch_size`, summed per part. The batch
  size is *derived* at submit (`2 * (jobs_max_concurrent // nparts) + 1`, which
  is 11 for 1000 jobs over 180 parts) and the value in `config.json` is
  overwritten; `submit --jobs-batch-size N` overrides it, subject to the same
  floor (`jobs_batch_unit_size`) and pool clamp. Measured on a campaign with 19
  parts in flight: 11 -> 102 forks, 25 -> 54, 50 -> 34, 100 -> 19. A batch holds
  its slots until its slowest job ends (max/mean wall time 1.15–1.5 in 23-job
  clusters), so 25–50 is the sensible range.
* **One `condor_q -json <cluster>` per fork per `htcondor_poll_time`.** With two
  campaigns at ~100 forks and 100 s, that is one `condor_q` process start per
  second on the login node, and four or five in flight at any instant. A single
  poller per campaign asking `condor_q` once for all clusters would remove almost
  all of it; it is an architectural change to `HTCondorExec`, not a setting.
* **Every fork inherits the parent's pipe fds** (~2 per sibling), which is why the
  open-file limit is raised at startup. Harmless for memory, but it is why the
  soft limit has to be ~10 x `nworkers`.

The mitigation that used to be mandatory — restart every 8–12 hours — is no
longer needed for memory. A restart before a final merge is still cheap and still
where accumulated fork drift does the most damage, so it remains good practice on
a very long campaign.

Note that the two costs left above — the fork population and the `condor_q` poll
rate — both exist only because the orchestrator stays alive to track jobs.
`tick_mode.md` proposes removing that requirement entirely, which would retire
both rather than tune them.

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
