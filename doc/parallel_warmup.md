# Parallel warmups with the kakuhen Basin integrator

Branch: `with_kakuhen` (commit `f0ada14`, September 2026).

## Motivation

Up to version 1.0.2 a warmup step of a part was a single NNLOJET job run as
`warmup = NCALL[NITER]`: the integrator adapts the grid after every iteration
and writes the state file, which dokan copies into the directory of the next
step. Warmups were therefore strictly sequential per part.

NNLOJET now integrates with the kakuhen Basin integrator (`extern/kakuhen`,
`integrator_id = 2` by default) which supports a parallel warmup pattern:

- `warmup = NCALL[NITER,noadapt]` (kakuhen "stage 3"): iterate on a *fixed*
  grid and dump the accumulated grid data of the run to
  `<PROC>.<RUN>.y<TCUT>.<CONTRIB>.s<SEED>.khd` (cumulative, rewritten after
  every iteration). The grid state itself,
  `<PROC>.<RUN>.y<TCUT>.<CONTRIB>.khs` (no seed in the name), is loaded when
  present and created from the default grid when absent.
- `NNLOJET --adapt -i <GRID>.khs -d <f1>.khd <f2>.khd ...` folds the data
  files into the state file and adapts the grid in place; the previous state
  is kept as `<GRID>.khs.bak`. The `-d` option is greedy and must come last.

With this, one warmup step can run `N` seeds in parallel in one directory,
followed by a single `--adapt`. This document describes how dokan does it.

## Workflow

```
step 1  (single seed, creates the grid)
  raw/warmup/<Part>/s1/        job.run: warmup = 500[2,noadapt]
                               -> X.khs (default grid), X.s1.khd
                               NNLOJET --adapt -i X.khs -d X.s1.khd
                               -> X.khs (adapted), X.khs.bak

step 2  (N seeds, N = run.jobs_batch_size)
  raw/warmup/<Part>/s2-4/      input: X.khs copied from step 1
                               job.run: warmup = 2000[2,noadapt]
                               seeds 2,3,4 -> X.s2.khd X.s3.khd X.s4.khd
                               NNLOJET --adapt -i X.khs -d X.s2.khd X.s3.khd X.s4.khd
                               -> X.khs (adapted), X.khs.bak
...
production                     input: X.khs of the last DONE warmup step
```

Facts the design relies on:

- `.khd` files are seed-tagged, so the existing filter in
  `DBRunner._prepare_execution` never copies them forward; the `.khs` is not
  seed-tagged and propagates exactly as before.
- In WARMUP mode `ExeData.scan_dir` keeps the input files in `output_files`;
  this is the mechanism by which the (adapted) `.khs` of step `n` becomes the
  input of step `n+1` and of production. Unchanged.
- The per-seed log has the same iteration summary block in stage 3 as in
  stage 1, chi²/iteration is 0 for a single iteration, and the `Elapsed time`
  line is printed by the main program. `nnlojet.parse_log_file` and
  `DBTask._update_job` therefore work unchanged.
- A runcard with `noadapt` warmups may contain no other sweep. dokan writes one
  sweep line per job runcard; the `init` dry run keeps
  `warmup = 1[1]  production = 1[1]` (stage 1).
- The very first step of a part must run a single seed: no `.khs` exists yet
  and stage 3 creates one when missing, which parallel seeds sharing a
  directory (LOCAL, Slurm) or transferring it back (HTCondor) would race on.

## Changes by file

### `warmup.py` — `combine_warmup_jobs`

Pure function folding the seeds of one step into *single-job-equivalent*
numbers so that `assess_warmup` and `size_preproduction` are untouched:

| field | value | why |
|---|---|---|
| `size` | `(ncall, max niter)` of one job | runtime estimates and `ntot` are per job |
| `elapsed_time` | slowest seed | per-job wall time vs. `job_max_runtime` |
| `result` | inverse-variance weighted mean over seeds with `error > 0` | |
| `error` | `sqrt(n / Σ 1/σ_i²)` = combined error × √n | the error one job of that size would have; keeps the √N scaling and the pre-production sizing dimensionally right |
| `chi2dof` | `Σ (x_i − x̄)²/σ_i² / (n−1)` for `n > 1` | between-seed consistency |
| `id` | smallest job id of the step | |

A single seed is returned as is. If every seed is `0 ± 0` the result is the
plain mean with error and chi2dof 0. Consequences: RELACC is judged on the
single-job-equivalent error and is therefore conservative by √n with respect
to the true combined accuracy of the step; CONST_ERR now measures the
stability of the error estimate on a fixed grid within a step (the errors of
all seeds' iterations are concatenated; the criterion is a relative spread and
scale-free).

### `preproduction.py` — steps instead of jobs

- `_queue_job(..., n=1)` creates `n` identical warmup rows in one commit and
  returns the first id.
- `_warmup_step_size`: 1 when no successful warmup exists (no `.khs` yet),
  otherwise `min(run.jobs_batch_size, run.jobs_max_concurrent,
  run.jobs_max_total)` so a step can never out-size the executor pool.
- `_successful_warmups` returns steps (`list[list[Job]]`), grouped by
  `rel_path` (all seeds of a step share the batch directory), most recent
  first, degenerate rows (`ncall*niter == 0`) excluded.
- `_assess_warmup` combines each step with `combine_warmup_jobs` and returns
  the steps alongside the assessment; `_iteration_errors` concatenates the
  per-iteration errors of all seeds from the single `ExeData` of the step.
- `_production_step` sizes the pre-production from the combined last step.
- `_active_job_id` and `_dispatch_then_resurrect` are unchanged: the oldest
  active job's `rel_path` is the batch directory, so one resurrection covers
  the whole step.

### `db/_dbdispatch.py` — one batch per step

Bounded dispatch (`DBDispatch(id=<job id>)`) of a WARMUP job now selects all
QUEUED warmup seeds of the same part and submission (`run_tag`), so they are
seeded contiguously and handed to one `DBRunner` (one directory `s<a>-<b>`).
Production dispatch, `select_job` and `complete()` are untouched: all rows flip
to DISPATCHED in the same commit, so "job `id` no longer QUEUED" still means
"step dispatched".

### `db/_dbrunner.py`

- Sweep line for warmups: `warmup = NCALL[NITER,noadapt]` (production
  unchanged).
- The grid copy skips `*.khs.bak`, `*.khs.new` and `*.khs.old`, so a later
  `DBDoctor` rescan can never propagate a backup or temporary state file.

### `exe/_executor.py` — the adapt step

`Executor.adapt_warmup_grids(exe_data, log)` runs in `Executor.run()` after
the final directory scan and before `finalize()`, for every policy (local,
HTCondor, Slurm) and for active-mode resurrection, on the submit host:

1. Only for `mode == WARMUP`; for every `*.khs` in the directory.
2. Skip when `<GRID>.khs.bak` exists: the grid was already adapted and a
   second `--adapt` with the same data would fail on the grid-hash check
   (every `.khd` records the hash of the grid it was produced with).
3. Use only the `.khd` files of seeds that have a parsed `result`, to exclude
   truncated files of killed processes. No usable file: log a warning, skip.
4. Remove a stale `<GRID>.khs.new` (left by an interrupted adaption; NNLOJET
   refuses to run while it exists).
5. Run `NNLOJET --adapt -i <GRID>.khs -d <khd...>` with `cwd` = job
   directory, `OMP_NUM_THREADS=1`, output captured into `exe.log` (which
   `DBRunner` dumps into the workflow log).
6. Success = return code 0 **and** `.bak` present, because NNLOJET ends usage
   errors (missing `-i`, empty `-d`) with a plain `stop`, exit code 0.
   Failure raises `RuntimeError`: the task fails loudly, a resubmission
   resurrects the step and retries.

No rescan after the adapt, so the `.bak` never enters `output_files`.

### `db/_dbresurrect.py` — passive recovery

`submit` first runs a passive `DBResurrect(recover_jobs=...)` for jobs left
active by a dead submission; that path never runs an Executor. It now calls
`Executor.adapt_warmup_grids` before finalizing, but only once all tracked
jobs are terminated, so a step whose seeds had finished before the crash is
adapted rather than silently propagating an un-adapted grid.

## Configuration

No new key. Relevant existing keys:

| key | role |
|---|---|
| `warmup.niter` | iterations per seed on the fixed grid; set to 1 for exactly `[1,noadapt]` |
| `warmup.ncall_start`, `fac_increment`, `min/max_increment_steps`, QC keys | unchanged semantics, now applied per step |
| `run.jobs_batch_size` | seeds per warmup step. Note: `submit` recomputes it from `jobs_max_concurrent`, `jobs_max_total` and the number of active parts (`__main__.py`, "clamp the batch size"), so with many parts it is small (1 in the 48-part WJunsym test) and it is steered through `--jobs-max-concurrent` |

Grid adaptation now happens once per step instead of once per iteration.

## Integrator choice: Basin or Vegas

Nothing in dokan depends on the integrator. The runcard line
`integrator = BASIN[ndiv1=..,ndiv2=..]` or `integrator = VEGAS[ndiv=..]` is
kept verbatim by the runcard template (only `iseed`, `warmup`, `production`
and the dokan placeholders are stripped), both kakuhen integrators write the
same `.khs`/`.khd` files, and `NNLOJET --adapt` selects the algorithm from the
state-file header. Verified end to end with `integrator = VEGAS[ndiv=80]` on
the eeJJ example (log: "Integrating using Vegas algorithm, stage 3",
`--adapt`: "adapt called for Vegas"). The legacy Fortran `Vegas.f90` is dead
code in NNLOJET and is not supported.

## Logging

The `--adapt` command line and NNLOJET's output are logged at DEBUG level in
`exe.log` (which `DBRunner` forwards to the workflow log); on success a single
INFO line `adapt: <GRID>.khs <- N data file(s)` is emitted, on failure the
full output at ERROR.

## Merge stage: findings from the WJunsym test run (not warmup related)

The first full run of `test/flav_release/WmC_ATLAS_2026.run` completed all
warmups and pre-productions and then appeared to hang in `MergePart`. Two
independent causes, both pre-existing on the `hdf5` branch:

1. **Single-file histogram output was unsupported.** `HISTOGRAMS > histos`
   makes NNLOJET write all observables of a job into one
   `<PROC>.<RUN>.<CONTRIB>.histos.s<SEED>.dat`, each block introduced by
   `#name: <obs>`. `build_obs_group` raised
   `NotImplementedError("single_file option not implemented yet")` (the older
   merge on `main` supported it). Now implemented: `_open_dat(path, obs_name)`
   yields either the whole file or one observable's block, every observable is
   mapped to the same job files, and the `MergePart` resume check is keyed per
   observable as well.
2. **Failures looked like a hang.** Luigi prints task exceptions to the
   console only (hidden behind the live monitor) and retries a failed task
   after `retry_delay` (900 s by default), so 48 failing `MergePart`s looked
   stalled. `MergePart` now logs a staging failure to the workflow log at
   ERROR before re-raising, and the parser reports malformed rows with file,
   observable and row number instead of a bare `AssertionError`.

Still open, on the NNLOJET side: the runcard's
`cross > cross_1j_IFN_neg nbins = 36 min = 27 max = 207` histograms (8 of
them) are written with a total-cross-section header (`#nx: 0`, no bin-edge
labels) followed by one total row *and* 36 binned rows with three bin-edge
columns; this happens for per-observable files too (`driver/core/Histograms.f90`).
The merge cannot interpret such a block and now fails with an explicit
message; those histograms must be declared as regular binned observables or
dropped from `config.json` until the writer is fixed.

## Edge cases

- Partial seed failures: FAILED rows are excluded from the QC, the adapt uses
  only seeds with a result; "last DONE warmup" lands in the same directory.
  All seeds failed: no adapt, the step is absent from the history and the
  same size is prescribed again (existing retry semantics).
- Crash during the adapt: `.khs.new` is removed on retry. Crash inside
  NNLOJET's two-rename publish window (`.khs` missing, `.bak` and `.new`
  present): restore by hand with `mv X.khs.new X.khs`.
- Re-`submit` with a queued but undispatched step: purged and re-queued.
- `DBRemoveJob` on one seed removes its `.khd`; the `.khs` already folded
  that data.
- `--no-warmup`: one single-seed step still runs when a part has no grid,
  then the QC is skipped. `--warmup`: the restart step uses
  `jobs_batch_size` seeds since a grid exists.

## Verification performed

- NNLOJET alone: two `noadapt` seeds, `--adapt` merged them in place and left
  a `.bak`; a second `--adapt` failed with "hash value mismatch" (exit 1).
- End-to-end LOCAL run of `examples/eeJJ/eeJJ_incl.run` at LO with three
  concurrent jobs: step 1 ran seed 1 alone, step 2 ran seeds 2–4 in one
  directory with the step-1 grid as input, each step adapted exactly once;
  md5 sums show the adapted grid flowing step 1 → step 2 → production;
  `.bak` never appears in input or output lists; QC terminated with
  RELACC|CHI2DOF|CONST_ERR|GRID|SCALING|MIN_INCREMENT.
- Recovery: with the seeds finished and the adapt "not yet run", a resubmit
  adapted through the passive path and produced a grid byte-identical to the
  uninterrupted run.
- HTCondor and Slurm backends were not exercised (no cluster available); they
  share the `Executor` base, so the adapt runs on the submit host after job
  tracking completes.

## Not done / follow-ups

- `WarmupFlag.GRID` and `nnlojet.grid_score` remain stubs; a real
  grid-convergence criterion could now read the `.khs` (`kakuhen dump`).
- A dedicated `warmup.njobs` key would decouple the warmup seed count from the
  production batch size if the automatic `jobs_batch_size` proves too small.
- The venv holds a non-editable copy of dokan; reinstall the package after
  checking out the branch.
