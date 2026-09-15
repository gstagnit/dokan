# `nnlojet-run tick`: running without a live orchestrator

A design plan, not an implemented feature. It records why the always-on
orchestrator is the wrong shape for a shared login node, what would replace it,
what the replacement costs (measured), and which parts are not yet solved.

## The problem

`submit` runs until the campaign finishes. On a shared interactive node that is a
poor fit:

* the node may be rebooted or the session disconnected, and the campaign stops;
* it forks a tracking process per in-flight batch, and that fork population --
  together with one `condor_q` per fork per poll interval -- is the dominant
  remaining footprint on the node, and what draws complaints from site
  monitoring (see `orchestrator_memory.md`);
* if those tracking processes are killed, their jobs stay `RUNNING` in the
  database forever and the dispatcher throttles against phantoms -- a campaign
  has sat wedged for nine hours this way with one real batch job left.

Every one of those is a consequence of *staying alive*, not of the work itself.

## The reframe

**The database is a cache. The truth is on disk and in the batch system.**

The evidence is already in the code: `doctor --scan-dir` reconstructs job state
entirely from the execution directories, and `_update_job(..., skip_terminated=
False)` will promote even a row previously written off as `FAILED` once its
output appears. Nothing about the state is lost when the orchestrator dies; it is
merely not being read.

Once that is accepted, continuous tracking is unnecessary. The orchestrator does
not need to *watch* jobs finish. It needs to *ask*, periodically, what finished.

## What a tick does

`nnlojet-run tick RUN` performs every action that is possible without waiting,
then exits:

1. **Reconcile** -- incrementally, from directories changed since the last tick.
2. **Merge** the parts that gained data.
3. **Re-optimise** the error budget and time allocation.
4. **Dispatch** up to the concurrency limit.
5. **Exit.**

Run it from a scheduler every 15-30 minutes. At CERN that means `acron` rather
than `crontab`, because it carries a valid Kerberos/AFS token.

## Measured cost

Reconciliation was the main risk, so it was prototyped and timed on a campaign of
2592 job directories / ~80k files / ~16k job rows, on AFS.

| stage | per job | full scan (~20k jobs) | one tick (~60 jobs) |
|---|---|---|---|
| find changed directories | -- | 0.3 s | 0.05-1.0 s |
| `ExeData` reconstruction | 6.1 ms | 366 s | 1.8 s |
| DB query + commit | 0.02 ms | 0.4 s | 0.003 s |
| **total** | | **~370 s** | **~2 s** |

**A tick reconciles in about two seconds**, against six minutes for the full scan
`doctor --scan-dir` performs -- and it scales with jobs completed since the last
tick, not with jobs ever run, so it does not degrade as a campaign grows.

Three observations that shaped the design:

* **Directory mtime is an exact change signal.** With `when_to_transfer_output =
  ON_EXIT_OR_EVICT` the batch system writes nothing into the execution directory
  until a job exits, so a directory's mtime changes precisely when there is
  something new to reconcile.
* **Directory reads are cheap on AFS; file reads are not.** Walking 2592
  directories takes 0.04 s. Stat-ing the 80k files inside them takes 21 s.
  Reading ~10k small output files takes 375 s. The design should touch as few
  files as possible, and one `os.listdir` per changed directory yields everything
  needed to decide which files matter.
* **The database is free.** 0.02 ms per row. It is never worth optimising.

`ExeData.scan_dir(force=True)` at 133 ms per 23-seed batch is the whole cost.
Most of it is re-examining seeds that were already terminal, which a tick can
skip by intersecting the changed directories with the non-terminal job rows.

## Design

### The stamp

A file in the run directory whose mtime marks the last successfully completed
reconciliation.

* Read the stamp, scan for directories newer than it, reconcile, commit, **then**
  advance the stamp.
* A tick that dies leaves the stamp alone, so the next tick re-reconciles the
  same window. Reconciliation is idempotent, so this costs only time.
* Use `find -newer <stampfile>`, never a relative date. `find -newermt "-10
  minutes"` silently matches nothing -- it returns no error and no results, which
  is easy to mistake for "nothing changed".

A periodic full `doctor --scan-dir` remains the backstop for anything a
mtime-based window could miss (clock skew, a directory written during a crash).

### Locking

Ticks must not overlap. An exclusive `flock` on a file in the run directory, with
the lock released on exit and a stale-lock timeout so a killed tick cannot block
the campaign permanently. A tick that cannot take the lock exits quietly -- the
previous one is still working, and the next scheduled tick will pick up.

### In-flight count from the batch system

Today the dispatcher throttles on a count derived from job rows, which is what
goes stale and wedges. A tick should instead ask the batch system directly, once,
tagging its jobs so they can be counted:

```
condor_q -constraint 'JobBatchName == "dokan-<run>"' -totals
```

This cannot go stale, because it is re-read every tick. It removes the entire
class of wedge, and it removes the need for a tracking process per batch -- which
is the fork-per-batch model, and with it the memory growth and the OOM exposure.

### Making the orchestrator return

The only thing keeping the process alive is
`DBDispatch._with_dispatch_continuation()`, which re-yields `DBDispatch(id=0,
_n+1)` after every round; the sole exit is `_dispatch_settled()`. A tick needs a
mode in which the dispatcher, having nothing left to do that does not involve
waiting for the batch system, simply returns -- at which point `luigi.build()`
completes and the process exits.

Luigi still has a job *inside* a tick: parallelising merges across local cores.
It just stops living between ticks.

### Warmup

Warmup is iterative -- each step's size depends on the previous step's quality
checks -- so a naive tick advances one step per interval. At ~9 steps and a
20-minute tick that is three hours of added latency before production starts.

Options, in order of preference:

1. let a tick keep working while it *can* make progress, and exit only when
   everything is waiting on the batch system (a warmup step whose jobs are still
   running blocks, but other parts continue);
2. a shorter tick interval during the warmup phase;
3. keep a live process for warmup only, and switch to ticks for production.

This is the least settled part of the design.

### Termination

A tick evaluates the accuracy target and the budget exactly as the live loop
does, and writes the dispatch-done signal when either is met. The final merge
stays a separate explicit `finalize`, because it is a heavy full re-merge and
does not belong in a periodic job.

## Scope of the change

Contained, because most of the machinery already exists and is already
idempotent:

| needed | status |
|---|---|
| incremental reconciliation | prototyped; ~2 s per tick |
| dispatcher returns instead of re-yielding | one function |
| in-flight count from the batch system | new, small |
| stamp + lock | new, small |
| merge resumable across processes | **already works** (markers, in-progress sentinel) |
| job state rebuildable from disk | **already works** (`doctor --scan-dir`) |
| resumption from the database | **already works** (exercised constantly) |

## What is lost

The live status board. Replaced by a `status` subcommand reading the database,
which is arguably better: it can be run from anywhere at any time, rather than
requiring an attached terminal on the node that happens to be running the
campaign.

Some cluster duty cycle: a job finishes and its slot sits idle until the next
tick refills. At a 20-minute tick against multi-hour jobs this is a few percent,
against restarts that have cost far more.

## Alternatives considered

* **Batch-system post-scripts updating the database directly.** SQLite over a
  network filesystem, written concurrently from many worker nodes. Locking is
  unreliable there; this would corrupt the database.
* **The orchestrator as a batch job.** Worker nodes generally cannot submit, and
  it trades a login-node dependency for queueing latency.
* **A supervised service with automatic restart.** Survives disconnection but not
  the memory growth, and does not address the site's objection to long-running
  interactive processes. It treats the symptom.

## Open questions

* Can warmup be made tick-friendly without a large latency penalty?
* `ExeData.scan_dir` re-examines terminal seeds; how much of the 133 ms per batch
  survives once the non-terminal intersection is applied?
* What is the right backstop interval for a full `doctor --scan-dir`?
* Should a tick ever block briefly (seconds) to let a nearly-finished merge
  complete, or always return immediately?
