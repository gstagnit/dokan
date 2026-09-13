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

## Failure mode 3: the stall guard cannot see a crash

`MergePart.run()` writes `raw/<part>.merge-guard.json` — a fingerprint of the
pending `MergeObs` set — and then yields those tasks. If an identical pending set
comes back, it concludes the merge ran and failed to converge, and raises
`internal invariant violated: re-merge of unchanged pending set ...`.

The guard write *must* be pre-yield: luigi restarts `run()` from the top after
dynamic dependencies complete, so nothing after the `yield` executes in that
invocation. A pre-yield fingerprint consequently cannot distinguish

- *MergeObs ran and produced outputs its own `complete()` rejects* — the real bug
  the guard hunts — from
- *the process died before MergeObs ran*.

Any event that kills workers mid-merge — an OOM burst, `SIGKILL`, a hard
interrupt — therefore leaves guards behind, and the **next** start aborts every
affected part immediately with the invariant error. The false positive is
acknowledged in a code comment, but nothing detects or clears it automatically.

**Workaround.** Any `raw/*.merge-guard.json` present while no run is active is by
definition stale; removing it lets the part retry:

```bash
ls -l raw/*.merge-guard.json      # inspect first
rm raw/*.merge-guard.json         # only with no run active
```

Before doing so, confirm the parts are healthy rather than genuinely
non-converging: their HDF5 caches should contain a full complement of objects,
and `result/part/<part>/` should be *empty*. A real non-convergence leaves `.dat`
files that fail the freshness check; zero files means `MergeObs` never ran.

**Proper fix (not yet implemented).** Move the invariant to where the evidence
is: at the end of `MergeObs.run()`, assert `self.complete()` and raise with
`describe_incomplete()` if it fails. The per-part `MergePart_{part_id}` mutex
guarantees no concurrent writer, so the identity cannot shift underneath. This is
correct in both directions — a real non-convergence fails immediately in the task
that caused it, while a crashed `MergeObs` is just a failed task luigi retries —
and it makes the guard, and the whole `merge-guard.json` stale-state class,
redundant. Failing that, the guard could gate its raise on every pending
observable having its `MergeObs.file_record` sidecar, i.e. on them all having
actually run.

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

## Open items

1. **Move the stall invariant into `MergeObs.run()`** (failure mode 3 above),
   retiring the merge-guard mechanism.
2. **Sweep stale guards at startup.** Any `raw/*.merge-guard.json` present when
   `submit` starts is from a dead process. The startup sweep already purges
   never-started jobs and prompts about FAILED ones.
3. **Persist luigi's own log** to `<rundir>/luigi.log` via a `FileHandler`, and
   reconsider `log_level="WARNING"` in `luigi.build` — it suppresses luigi's
   scheduler-level notices, which the event handlers do not replace.
4. **The fork-per-task memory model remains fragile.** The `merge_concurrent`
   cap addresses the merge fan-out, but any large fan-out of `DBTask`s forks the
   same parent, and `DBTask` is sized at `nactive_part + 2`. Worth understanding
   why the parent grows to several GB, and whether that is a leak: a smaller
   parent makes every concurrency limit less critical.
