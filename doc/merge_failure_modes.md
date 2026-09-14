# Merge failure modes

How the merge stage fails, why those failures are hard to see, and what to do
about them. Everything here concerns `MergePart` / `MergeAll` and the per-part
HDF5 staging cache `raw/<part>.hdf5`.

## The characteristic symptom: a merge that looks stalled

A dokan run whose merges are failing does not look like a run that is failing.
It looks like one that is working slowly:

- the status board keeps refreshing and the process keeps running;
- `log.sqlite` gains nothing for stretches of ~15 minutes, then a burst of
  `MergePart[...]::run: continue` lines, then silence again;
- `result/merge/cross.dat` never updates, i.e. `MergeAll` never runs;
- the board shows `PRD A[0/0]` for the affected parts and, while no merge has
  ever completed, `current "<opt_target>" error: inf%`;
- `ps` shows the luigi worker with exactly one live child — the `Monitor` task —
  plus unreaped zombies;
- the batch queue is empty.

`Entry` is parked at stage 2 (`complete preprods -> MergeAll`) and cannot
advance. The process does not exit either, because `Monitor` is deliberately
long-lived: it terminates only on a `SIG_COMP`/`SIG_TERM` log record
(`monitor.py`). **A permanently stalled run is indistinguishable from a healthy
one at a glance.**

The ~15 minute period is the tell. It is luigi's `retry_delay`, whose default in
luigi 3.x is 900 s; with `retry_count = 999999999` and `disable_failures = None`
a failing task returns on that cadence forever. Any gap of almost exactly 900 s
between bursts of activity means something is failing silently.

## Why the failures are silent

Luigi reports task failures through its own logger, to stderr. In a dokan run
that output is effectively invisible, for two independent reasons:

1. **Process topology.** Every task attempt runs in its own forked
   `TaskProcess`, while the `rich.Live` status board lives only in the `Monitor`
   task's process. A worker's traceback goes to the shared terminal and races
   with the board's redraw, which overwrites it. No amount of nicer printing
   from a worker fixes this — the board is in a different process.
2. **Log level.** `luigi.build(..., log_level="WARNING")` suppresses luigi's
   INFO-level notices, including `Task ... died unexpectedly with exit code`.

The log database is the only channel every forked worker shares, and `Monitor`
already streams it to the board. Failures therefore have to be written *there*
to be seen at all, and to remain readable once the run is over. Three luigi
event handlers in `db/_dbtask.py` do this:

| Event | Fires when | Catches |
|---|---|---|
| `FAILURE` | the task raised | ordinary task exceptions, with traceback |
| `PROCESS_FAILURE` | the worker died with no exception | hard kills: OOM, segfault |
| `BROKEN_TASK` | `complete()`/`requires()` raised | faults before any `run()` |

They are registered on the dokan `Task` base rather than `DBTask`, because
`MergeObs` is a plain `Task` — deliberately DB-free, so the standalone
`nnlojet-merge` tool can use it — and its failures matter just as much.

Two properties of that code are load-bearing and should not be "simplified"
away:

- **The handler must never raise.** Luigi triggers `FAILURE` from inside its own
  `except` block (`TaskProcess._handle_run_exception`); an exception escaping
  the handler replaces the original failure and leaves the worker's result
  handling inconsistent. Every error is swallowed, with the console as a
  last-resort sink.
- **Tracebacks are truncated from the front**, not the back: the raising frame
  is the informative end.

## Failure mode 1: the merge fan-out exhausts memory

`__main__.py` sizes the worker pool as `max(cpu_count, nactive_part) + 1` and
declares `DBTask` as `nactive_part + 2`. Without a further limit nothing
throttles how many `MergePart` tasks run at once, and the moment every
pre-production completes, `MergeAll.requires()` returns one `MergePart` per
active part and luigi forks them all.

luigi forks one process per task attempt, and copy-on-write does not help:
CPython touches refcounts across the heap, so each child materialises close to
the parent's full resident size. A parent that has grown to a few GB over a long
run therefore multiplies by the number of parts, and on a shared machine with a
per-user cgroup memory limit the kernel OOM-kills the children — replacements
included, as luigi keeps forking into the shortfall.

**Mitigation.** `MergePart` takes a `merge_concurrent` resource slot, declared
for the `submit` and `finalize` builds as `min(#cores, 8)` and overridable with
`--merge-cores`. The startup banner prints the effective value as
`# merge cores:`.

Lowering `DBTask` instead would also throttle `DBRunner`'s batch-system polling
across parts and slow the whole workflow, which is why the cap is specific to
merges.

If OOM kills are suspected:

```bash
dmesg -T | grep 'Killed process'
cat /sys/fs/cgroup/user.slice/user-$(id -u).slice/memory.max   # cgroup v2
```

With the `PROCESS_FAILURE` handler in place these also appear in `log.sqlite` as
ERROR records at the moment they happen, which is the better signal.

## Failure mode 2: a killed writer damages the HDF5 staging cache

`build_obs_group()` opens `raw/<part>.hdf5` with mode `"a"` and sets
`h5f.swmr_mode = True`, which stamps "file is open for write" into the
superblock. A clean close clears it; a `SIGKILL` does not.

A writer killed mid-write leaves two distinct symptoms, and — this is the trap —
**each is visible on only one of the two access paths**:

| Symptom | Detected by | Raises |
|---|---|---|
| stale SWMR "open for write" flag | the *write* open only | `OSError` |
| objects past the recorded end-of-address | reading an object header | `KeyError` |

A *reader* opens a flagged file perfectly happily — that is exactly what SWMR is
for — so a read-only probe cannot detect the first symptom. And the second
leaves the file and its root link table readable, so the damage surfaces only
when an object is actually accessed:

```
KeyError: 'Unable to synchronously open object
  (len not positive after adjustment for EOA)'
```

Left unhandled, either symptom wedges the part **permanently**, because both
readers of the cache run *before* the ingest that would repair it: every retry,
and every subsequent run, dies in the same place.

**Mitigation.** `raw/<part>.hdf5` is *derived* data — every observable in it is
rebuilt from the raw `.dat` job output — so an unusable cache is always
recoverable and must never fail the part. No cheap up-front probe covers both
symptoms (it would need a write open, which touches the file's mtime and hence
its `MergeObs` freshness identity), so both paths recover at their point of use:

- the resume read in `MergePart.run()` catches `_HDF5_CACHE_ERRORS`, discards the
  cache and continues with an empty staged set;
- `MergePart._stage_histograms()` wraps `build_obs_group()`, discarding and
  retrying **exactly once** on a freshly created file. A second identical failure
  means the fault is not the cache, and propagates.

Both log a `WARN` naming the underlying error.

`_HDF5_CACHE_ERRORS` is deliberately narrow — `(OSError, KeyError)`. A
`ValueError` out of `build_obs_group()` (a binning mismatch between the runcard
and the staged cache) is a genuine inconsistency, not damage: it stays fatal and
the cache is left intact for inspection.

### Never run `h5clear` against a live run

`h5clear -s` is the documented way to clear a stale consistency flag, and the
HDF5 error message itself suggests it. **It is destructive if the file is
currently held by a writer**: clearing the flag under a live writer resets the
file's end-of-address and truncates it, converting symptom 1 into symptom 2 and
destroying the staged data.

A file showing the flag may simply be one that is being written right now. If
manual intervention is ever unavoidable: stop the run first, and confirm nothing
holds the files with

```bash
ls -l /proc/*/fd 2>/dev/null | grep hdf5
```

With the self-healing above, this manual surgery should not be necessary — the
merge repairs the cache on its own. Deleting the file outright is in any case
safer than `h5clear`, since it forces a clean rebuild from the raw `.dat`.

## Failure mode 3: a stall guard that cannot see a crash

`MergePart.run()` writes `raw/<part>.merge-guard.json` — a fingerprint of the
pending `MergeObs` set — and then yields those tasks. The write *must* be
pre-yield: luigi restarts `run()` from the top once dynamic dependencies
complete, so nothing after the `yield` executes in that invocation.

A pre-yield fingerprint therefore cannot distinguish

- *MergeObs ran and produced outputs its own `complete()` rejects* — the real bug
  the guard hunts — from
- *the process died before MergeObs ran*.

The guard originally raised on the first verbatim repeat, so any event that
killed workers mid-merge left guards behind and the **next** start aborted every
affected part with `internal invariant violated`, naming a bug that had not
occurred.

**How it works now.** The invariant is checked where the evidence is, and the
guard keeps only the job the check cannot do:

- **`MergeObs.run()` verifies its own `complete()` before returning.** It knows it
  ran, so a merge whose output its freshness model rejects fails immediately, in
  the task that caused it, with the same per-observable diagnostics and no 900 s
  detour. This is sound because the source HDF5 is opened read-only by every
  `MergeObs`, and the per-part `MergePart_{part_id}` resource keeps the one
  `MergePart` that writes it from running concurrently, so the identity cannot
  shift between the merge and the check.
- **The guard counts attempts** (`_MAX_MERGE_ATTEMPTS`) instead of tripping on the
  first repeat, warning on each. What is left for it is the one case the
  self-check cannot see — `MergePart` and `MergeObs` disagreeing about
  completeness, which would re-yield forever — so it only has to *terminate*, and
  a bounded retry does that while tolerating crashes. A crash now costs one
  attempt rather than aborting the part.
- **`submit` clears `raw/*.merge-guard.json` at startup**, so a count never
  carries across runs and residue from a dead run costs nothing.

If a part does exhaust its attempts, the error names the disagreement and the
guard path; deleting that file forces a retry. Before doing so, check whether the
part is genuinely stuck: `result/part/<part>/` empty with a healthy HDF5 cache
means `MergeObs` never ran, whereas a real non-convergence leaves `.dat` files
that fail the freshness check.

## Diagnostics

Is the workflow progressing, or cycling the backoff?

```bash
python3 -c "
import sqlite3, datetime
c = sqlite3.connect('file:log.sqlite?mode=ro', uri=True)
i, t = list(c.execute('select id,timestamp from log order by id desc limit 1'))[0]
print(i, datetime.datetime.fromtimestamp(t))"
```

Which parts have not merged (a complete part has one `.dat` per histogram):

```bash
python3 -c "
import glob, os
for d in sorted(glob.glob('result/part/*')):
    n = len(glob.glob(d + '/*.dat'))
    if os.path.isdir(d) and n == 0: print(os.path.basename(d), n)"
```

Unclean HDF5 caches, read straight from the superblock so no HDF5 lock is taken
and no flag is disturbed (byte 11 is the file-consistency flags of a v2/v3
superblock; non-zero means it was not closed cleanly):

```bash
python3 -c "
import glob
for f in sorted(glob.glob('raw/*.hdf5')):
    h = open(f,'rb').read(12)
    if h[8] >= 2 and h[11]: print(f, 'flags =', h[11])"
```

Remember this reports files that are legitimately open for writing right now,
too. It is only evidence of damage when no run is active.

Failures, once the event handlers are in place:

```bash
python3 -c "
import sqlite3, datetime
c = sqlite3.connect('file:log.sqlite?mode=ro', uri=True)
for i,l,t,m in c.execute('select id,level,timestamp,message from log where level>=40 order by id desc limit 20'):
    print(i, datetime.datetime.fromtimestamp(t).strftime('%H:%M:%S'), m[:200])"
```

## Why the parent interpreter grows

Failure mode 1 multiplies whatever the parent is holding, so it is worth knowing
where that came from. Two luigi properties combine:

- `Worker._scheduled_tasks` and `Worker._add_task_history` are **never pruned**.
  Every task instance the worker has scheduled is retained for the lifetime of
  the run, and the workflow creates one instance per dynamic clone — with many
  parts and repeated merges that reaches tens of thousands.
- `DictParameter.normalize()` **deep-freezes its input on every instantiation**,
  so each of those instances used to own a private copy of the run
  configuration. Measured at ~44 kB per copy, tens of thousands of instances is
  most of a GB, retained, in the process that every task fork then copies.

`Task.config` therefore uses `SharedDictParameter` (`task.py`), which hands out a
canonical frozen instance: the retained cost drops from ~0.9 GB to one copy.
Frozen values are immutable, so sharing is safe, and the value still compares
equal to what `DictParameter` produced, leaving task ids and the
`to_str_params()` round-trip untouched.

The retention itself is luigi's and remains: the task objects stay alive, they
are simply small now. If the parent is ever seen growing again, `_scheduled_tasks`
is the first place to look, and the question is what those instances are holding
rather than how many there are.

### What the fix is worth, measured

Three measurements, all with the shared-config parameter in place:

| | |
|---|---|
| parent holding 20 000 retained task instances | **105 MB** (was ~0.9 GB in configs alone) |
| child forked from that parent, after its own `gc.collect()` | **92 MB** — nearly all shared |
| same, with `gc.freeze()` before the fork | **91 MB** — no useful gain |
| RSS across six consecutive merges in one process | **71 → 72 MB**, +25 gc objects each |

The last row is the important one: the merge itself does not accumulate, so the
parent has no per-merge leak. The growth that made the fan-out dangerous was the
retained configuration, and it is gone. `gc.freeze()` before `luigi.build` was
tried and rejected on the evidence — with the copies removed there is nothing
left for it to protect.

This also settles the `DBTask` pool size, which is still `nactive_part + 2`: it
was only hazardous because each fork copied a multi-GB parent. Lowering it stays
rejected — it would throttle the batch-system pollers and slow the whole
workflow — and `merge_concurrent` remains the specific cap on the one fan-out
that is both wide and memory-hungry.

## Open items

Nothing outstanding from the failure modes above.

## Heavy-tailed parts: outlier trimming, and why it is mostly not what saves you

Double-real channels produce occasional events whose weight is orders of
magnitude above the bulk. A single seed can then return a value millions of
times the channel's true size, with a relative error near 100% — one event
carrying the whole job.

The merge has a defence: double-MAD trimming (`merge/_core.py`). Per bin it
takes the median of the per-dataset results, estimates a robust 1-sigma
separately below and above it (the distribution is skewed, so a two-sided MAD
avoids biasing rejection towards the longer tail), and forms a neval-weighted
robust z-score. Datasets above `trim_threshold` (default 8) are candidates.

### The cap makes it a no-op for a typical part

```python
max_trim = trim_max_fraction * ndat          # default 0.007
for ntrim, itrim in enumerate(np.argsort(-bin_buf1)):
    if bin_buf1[itrim] <= trim_threshold or (ntrim + 1) > max_trim:
        break
```

The loop breaks *before* the first trim unless `max_trim >= 1`, i.e. unless

```
ndat >= 1 / trim_max_fraction = 143      (at the default 0.007)
```

`ndat` is the number of seed datasets accumulated for that part, and 0.7% is a
fraction sized for NNLOJET's own combine, where thousands of files are merged in
one go. dokan merges per part, incrementally, where `ndat` is tens to low
hundreds. Below 143 datasets the detector runs, flags the outliers correctly,
and is then forbidden from removing any of them.

Check where a campaign sits:

```bash
python3 -c "
import h5py, glob
for f in sorted(glob.glob('raw/*.hdf5')):
    with h5py.File(f,'r',swmr=True) as h:
        for p in h:
            if 'cross' in h[p]:
                nd = int(h[p]['cross'].attrs['ndat_valid'])
                print(f'{p:10s} ndat={nd:5d}  trims={int(0.007*nd)}')"
```

### Statistics, not trimming, is what actually stabilises these parts

Worth knowing before reaching for the cap. Measured across two charge-conjugate
campaigns of the same process, matched part by part on `(contribution, channel
label)` — the conjugate of `[a,b]` being `[-a,-b]`, since `part_num` is a shared
index and the *same* `part_num` is a *different* initial state in each:

* violent outliers occur in **both**, at comparable rates (1.2% and 2.4% of
  datasets above z=8, peak z of 633 and 1132) — they are a property of the
  double-real subtraction, not of one charge;
* two parts carrying near-identical spikes (about -9.6e6 at z=633, and -8.6e6 at
  z=627) merged to `-230,957 +/- 271,892` and `-157 +/- 1,227` respectively;
* what separated them was `ndat`: 48 against 201.

With enough datasets the k-scan finds a plateau and the spike is diluted. With
too few it never plateaus, keeps calling `merge_pair()`, and converges on the
fully pooled estimate — where one event dominates. In the well-sampled case
trimming had removed exactly **one** dataset of seven flagged, so it was not the
mechanism doing the work.

The practical reading: a part whose error is orders of magnitude above its
neighbours is usually under-sampled rather than wrong, and the fix is seeds, not
rejection. Discarding large-weight events is not free either — they can be real
contributions that cancel against opposite-sign events of similar size, so
trimming a small sample can bias the answer rather than clean it.

> **Superseded.** The cap is now `max(1, trim_max_fraction * ndat)`, so the
> discontinuity described below is gone: removal no longer waits for a part to
> accumulate `1/fraction` datasets. `outlier_trimming.md` has the evidence that
> these outliers are matrix-element artifacts rather than physics, and why the
> question cannot be settled from the seed-value distribution alone.

### The discontinuity

Because the cap is a pure fraction with no floor, a part's central value can move
discontinuously the first time it crosses `ndat = 143` and a trim becomes
possible. A campaign in progress is generally split across that line, so parts of
one merge have had trimming applied and parts have not. Raising
`trim_max_fraction` in the runcard's `[Options]` (`trim = 8 0.05`) moves the
threshold rather than removing it; a floor of one trim above some minimum active
sample would remove it, at the cost of changing central values everywhere.
