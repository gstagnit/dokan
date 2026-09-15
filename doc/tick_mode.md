# `nnlojet-run tick`: running without a live orchestrator

Implemented (branch `tick-mode`).  This document records why the always-on
orchestrator is the wrong shape for a shared login node, what replaces it, how the
replacement differs from the plan it grew out of, and what is not yet settled.

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

1. **Lease** -- `tick.lease` in the run directory, so ticks never overlap.
2. **Reconcile** -- one batch-system query, then every in-flight batch is
   finalized (gone from the queue), advanced (seeds that finished early are
   marked done) or left alone (still queued).
3. **Merge** the parts that gained data, through the same `MergePart` /
   `MergeAll` tasks the live run uses, in parallel across local cores.
4. **Dispatch** -- the next warmup or pre-production step of every part still in
   that phase, or one production wave, through the same `DBDispatch` /
   `DBRunner` chain, with executors that submit and return.
5. **Finish** -- once dispatch is settled, the end-of-run merge and the
   completion signal, exactly as `Entry` stage 4 does.
6. **Exit.**

Run it from a scheduler every 15-30 minutes. At CERN that means `acron` rather
than `crontab`, because it carries a valid Kerberos/AFS token:

```
# acrontab -e   (the host field may name any login node; ticks tolerate that, see "Schedd")
*/20 * * * * lxplus.cern.ch /path/to/dokan-venv/bin/nnlojet-run tick /path/to/RUN --quiet >> /path/to/RUN/tick.log 2>&1
```

**The environment.** A tick inherits the environment of whatever launched it,
and a scheduler's is bare: no compiler module, no `LD_LIBRARY_PATH`, no
`LHAPDF_DATA_PATH`.  NNLOJET needs all of that twice over -- on the login node,
where the tick runs `NNLOJET --adapt` for every finished warmup step, and on the
worker nodes, which receive the tick's environment through `getenv = True` in the
submit template.  Launch the tick through a script that loads what your shell
loads:

```sh
#!/bin/bash
# tick.sh -- what ~/.bashrc does for NNLOJET, then one tick
source /cvmfs/sft.cern.ch/lcg/releases/gcc/15.2.0/x86_64-el9/setup.sh
export LD_LIBRARY_PATH=/path/to/LHAPDF/install/lib:$LD_LIBRARY_PATH
export LHAPDF_DATA_PATH=/cvmfs/sft.cern.ch/lcg/external/lhapdfsets/current
exec /path/to/dokan-venv/bin/nnlojet-run tick "$@"
```

```
*/20 * * * * lxplus.cern.ch /path/to/tick.sh /path/to/RUN --quiet >> /path/to/RUN/tick.log 2>&1
```

A tick checks first that the executable starts at all and aborts with the
loader's message otherwise, before touching anything; a batch whose collection
fails for another reason has NNLOJET's output echoed into the workflow log and is
retried on the next tick.

`nnlojet-run status RUN` prints the live board once, from the database, from
anywhere.  A tick's own messages go to the log database like the live run's and
are echoed on its stdout (`--quiet` keeps only the one-line summary).

A tick never prompts.  It refuses a `local` execution policy (there is no queue to
ask), and it refuses a configuration without a termination condition, as `submit`
does.  A run that was never submitted is initialised from the configured order,
as `submit` would do without a channel selection; an initialised run keeps its
parts as they are.

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

* **Directory reads are cheap on AFS; file reads are not.** Walking 2592
  directories takes 0.04 s. Stat-ing the 80k files inside them takes 21 s.
  Reading ~10k small output files takes 375 s. The design touches as few files
  as possible: one `os.listdir` per batch that finished, and the metadata file of
  every batch in flight.
* **The database is free.** 0.02 ms per row. It is never worth optimising.
* **The batch system knows.** One `condor_q` for everything of ours costs about
  what one of the live tracker's per-cluster polls does, and answers for every
  batch at once.

The merge step dominates a tick in practice, and it is the same work the live run
does at the same moments.

## Design

### What is in flight: the database, not a stamp

The plan proposed a stamp file and `find -newer` to locate directories changed
since the last tick.  That is not needed: the job table already lists the batches
in flight (`DISPATCHED`/`RUNNING` rows with a `rel_path`), and that set is small
-- at most `jobs_max_concurrent / jobs_batch_size` per part.  The tick loads the
metadata of exactly those batches (one small JSON each) and asks the batch system
about them.  Nothing else on disk is looked at, and there is no window that a
clock skew or a crash mid-write could make a tick miss.

A periodic full `doctor --scan-dir` remains the backstop for what the database
does not know about at all (a wiped table, batches from before the database was
initialised).

### In-flight count from the batch system

The live dispatcher throttles on a count derived from job rows, which is what
goes stale and wedges.  A tick asks the batch system, once per scheduler, and
matches every batch by its **working directory** (`initialdir` on HTCondor,
`WorkDir` on Slurm) rather than by cluster id:

```
condor_q -name <schedd> -nobatch -af:t GlobalJobId JobStatus Iwd
```

The directory is unique per batch by construction and is known *before* the
submission happens, so it also identifies a batch whose `condor_submit` succeeded
but whose cluster id was never written back (a tick killed between the two).
Such a batch is *adopted* -- its id recovered from the queue -- instead of being
submitted twice.

For every batch in flight, three outcomes:

| queue says | tick does |
|---|---|
| no job of the batch left | `Executor.finish()`: scan the directory, adapt warmup grids, write `job.json`; `_update_job` marks every seed `DONE`/`FAILED`; the part becomes a merge candidate |
| some seeds gone, some queued (production only) | seeds that are gone *and* have a parsed result are marked `DONE` now; the rest wait.  A seed gone without a result is left to the whole-batch collection above, which retries the directory scan against filesystem lag |
| everything still queued | held jobs are released, as the live tracker does; nothing else |

The early per-seed marking is what makes the in-flight count accurate: the
database tracks the queue seed by seed, and a finished seed's data joins the next
merge instead of waiting for the slowest job of its batch.  Warmup steps are
collected whole, because the grid adaption needs every seed's data at once.

This cannot go stale, because it is re-read every tick.  It removes the entire
class of wedge, and it removes the need for a tracking process per batch --
which is the fork-per-batch model, and with it the memory growth and the OOM
exposure.

### Schedd

lxplus attaches every login node to one of several schedds (`condor_config_val
SCHEDD_HOST`), so a `condor_q` issued on another node does not see what was
submitted here.  Every detached submission records its schedd in the batch
metadata (`htcondor_schedd`), and the tick queries each recorded schedd by name
plus the local one.  Batches submitted by the live orchestrator carry no record;
for those a single `condor_q -global` is added, and if that fails only they are
skipped for the tick (a named schedd that cannot be reached fails the whole
reconciliation, because "not in the queue" means "finished").

### Detached executors

`Executor.detached` (a Luigi parameter) turns `run()` into "stage, submit,
return": the tracking half is `Executor.finish()`, called by the reconciliation.
A detached executor is *complete* once its batch is submitted, so the
`DBRunner` above it continues past the yield, records nothing (the reconciliation
does), and returns.  A detached submission that fails is a task failure: the batch
stays staged on disk without a batch handle, and the next tick re-submits it.

The `jobs_concurrent` Luigi resource, which caps the live run's submitted jobs
because an attached executor holds its slots for as long as its batch runs, holds
nothing across a tick.  The cap is applied in the dispatcher instead
(`DBDispatch._free_slots`): a detached wave never pushes the database's in-flight
count -- per-seed accurate after the reconciliation -- past `jobs_max_concurrent`.
Rows the cap leaves `QUEUED` are the next tick's first wave; the dispatcher drains
them before planning new ones, as it always did -- which also means a changed
accuracy target takes effect once that backlog has drained, as in a live run.

### Making the orchestrator return

`config.run.detached` is set by the `tick` command (never persisted, like
`warmup.skip_qc`).  With it, `DBDispatch` does not re-yield `DBDispatch(id=0,
_n+1)` after a wave, returns instead of sleeping when throttled, and skips the
idle pause -- so `luigi.build()` completes when there is nothing left to do
without waiting for the batch system.  Luigi still has a job *inside* a tick:
parallelising the merges and the submissions across local cores.  It just stops
living between ticks.

### Warmup

`PreProduction.run()` yields a dispatch and then a *resurrection* of the step,
which would track it.  The tick reuses its decision methods directly instead:
`_warmup_step` / `_production_step` queue the next step when the part is ready
for one and name the job to dispatch, and the tick issues the bounded
`DBDispatch(id=job)` for it.  The QC decision, the seed sharing among the parts
still warming up, and the pre-production sizing are unchanged.

A naive tick therefore advances one warmup step per interval, and at ~9 steps
that is a few hours of added latency before production starts.  Of the options
considered -- keep working while progress is possible, a shorter interval during
warmup, a live process for warmup only -- the first is what the tick does within
a tick (a finished step is collected, assessed and its successor submitted in the
same tick), and the second is a scheduling choice: a 5-minute interval costs one
`condor_q` and a few database reads per tick while nothing is finished.

### Campaign tag

A live `submit` mints a fresh `run_tag` per process, and everything the
dispatcher counts is scoped to it.  Ticks are many short processes standing in
for one long one, so they share a tag, kept in `tick.json`.  Consequences:

* `jobs_max_total` counts over the whole tick campaign, not per tick;
* active batches of an earlier `submit` (or of an earlier tick campaign) are
  adopted into the tag, so the dispatcher counts them and settles on them --
  including their production jobs, which `submit` would keep on the old tag;
* queued rows of another tag are purged at the start of a tick, the way `submit`
  purges rows that never started;
* deleting `tick.json` starts a new tag, i.e. a new "submission".

The two commands compose: a tick after a dead `submit` picks up its batches, and a
`submit` after a tick campaign resurrects the tick's batches through the usual
startup pass.

### Termination

A tick evaluates the accuracy target and the budget exactly as the live loop
does -- it *is* the live loop's `_repopulate` -- and writes the dispatch-done
signal when either is met.  Once no active job remains, the tick runs
`MergeFinal`: the forced merge of what is done plus the per-order files and the
completion signal, as `Entry` stage 4 does.  Later ticks then do nothing.  The
from-scratch re-merge with the k-scan and the grids stays the explicit
`finalize`, because it is heavy and does not belong in a periodic job.

`tick --reopen` demotes the terminal signals so a finished campaign continues
after the budget was raised or the target lowered in `config.json` (the live
`submit` achieves the same by clearing the log at startup).  `tick --no-dispatch`
only reconciles and merges: a campaign is drained by switching to it.

### Lease

`tick.lease` is created with `O_EXCL`, which is atomic on every filesystem dokan
runs on.  `flock` is not relied upon: ticks may run on different hosts, and
advisory locks across AFS clients are not something to build a campaign on.  The
lease is taken over when its owner is a dead process on this host, or when it is
older than `--lease-timeout` (3 h by default: a tick that died on another host, or
one hung beyond what a tick may reasonably take).  A tick that cannot take the
lease exits quietly -- the previous one is still working, and the next scheduled
tick will pick up.

## Scope of the change

| needed | status |
|---|---|
| incremental reconciliation | `Tick.reconcile`, from the in-flight set |
| dispatcher returns instead of re-yielding | `DBDispatch.detached` |
| in-flight count from the batch system | `Executor.queue_snapshot` / `seeds_in_queue`, HTCondor and Slurm |
| lease + campaign tag | `TickLease`, `campaign_tag` |
| merge resumable across processes | **already worked** (markers, in-progress sentinel) |
| job state rebuildable from disk | **already worked** (`doctor --scan-dir`) |
| resumption from the database | **already worked** (exercised constantly) |

Verified end to end against a fake HTCondor (`tests/test_tick.py`): warmup steps,
pre-production, the forced pre-production merge, capped production waves, early
seeds, held jobs, a failed submission retried, a lost cluster id adopted, a
scheduler outage aborting before anything is touched, settlement, completion and
re-opening.  The Slurm backend mirrors the HTCondor one and is untested against a
live scheduler.

## What is lost

The live status board. Replaced by `status`, which is arguably better: it can be
run from anywhere at any time, rather than requiring an attached terminal on the
node that happens to be running the campaign.

Some cluster duty cycle: a job finishes and its slot sits idle until the next
tick refills. At a 20-minute tick against multi-hour jobs this is a few percent,
against restarts that have cost far more.  The per-seed marking keeps it to
that: a slot is refilled at the tick after its job ends, not after its batch ends.

## Alternatives considered

* **Batch-system post-scripts updating the database directly.** SQLite over a
  network filesystem, written concurrently from many worker nodes. Locking is
  unreliable there; this would corrupt the database.
* **The orchestrator as a batch job.** Worker nodes generally cannot submit, and
  it trades a login-node dependency for queueing latency.
* **A supervised service with automatic restart.** Survives disconnection but not
  the memory growth, and does not address the site's objection to long-running
  interactive processes. It treats the symptom.
* **A stamp file and `find -newer`.** Superseded by the in-flight set (above); it
  would also have had to handle `find -newermt "-10 minutes"` silently matching
  nothing, which is easy to mistake for "nothing changed".

## Open questions

* The warmup latency: whether a shorter interval during warmup is enough, or a
  tick should be allowed to wait briefly (seconds) for a warmup step that is
  about to finish.
* Failed rows accumulate in tick mode (`submit` offers to remove them at
  startup); `doctor` is the place to clean them, and nothing else reads them.
* What the right backstop interval for a full `doctor --scan-dir` is.
* Whether a tick should ever block briefly to let a nearly-finished merge
  complete, or always return immediately.
