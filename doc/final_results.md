# `result/final`: the per-order results, and when they appear

## What is in there

`result/` holds two different things:

| path | content |
|---|---|
| `result/part/<part>/<obs>.dat` | one merged file per part — a single partonic channel of a single contribution |
| `result/<obs>.dat` | every active part summed: the full result at the order the run was set up for |
| `result/final/<order>.<obs>.dat` | one file per **perturbative order** |

Only the third gives the lower orders. `Order` spans `lo`, `nlo`, `nnlo` and the
coefficient-only variants `nlo_only`, `nnlo_only`, and the selection is by part
order: a cumulative order takes every part with `abs(part.order) <= order`, a
`_only` order takes just `part.order == order`. So `lo.<obs>.dat` is the LO
prediction, `nlo.<obs>.dat` the full NLO, `nlo_only.<obs>.dat` the NLO
coefficient alone — from the same jobs, at no extra integration cost.

An order is skipped, with a message, when any of its parts has `ntot <= 0`: a
cumulative order needs *all* its parts, so a single un-merged part suppresses
that order until it arrives. An order with no parts at all (NNLO for an NLO-only
process) is skipped silently.

## When they are written

`MergeAll` writes them, but only when its `finalize` parameter is set, which
happens on three paths:

* `MergeFinal` requires `MergeAll(finalize=True)`, so the end of a completed run
  always produces them;
* the explicit `nnlojet-run finalize RUN` CLI path;
* a `nnlojet-run signal RUN merge` request, consumed by the dispatcher.

Everything else — the `MergeAll` that `MergePart` yields after each part merge,
which is what drives the periodic cross-section report — leaves `finalize` unset
and writes only `result/<obs>.dat`.

## `finalize_interval`

`run.finalize_interval` (`--finalize-interval`, accepting `1h`, `30m`, …)
refreshes `result/final` on a timer while the run is going, so the lower orders
are readable long before the campaign ends. `0` restores the previous behaviour
of writing them only at the end.

Two properties are worth stating, because both come from *where* the trigger
lives rather than from an explicit check:

* **Production only.** The trigger sits on the dynamic dispatcher, and
  `DBDispatch(id == 0)` is yielded by `Entry` stage 3 alone. Warmup and
  pre-production drive their own bounded dispatches (`id != 0`), so they can
  never fire one — which matters, since a finalize during pre-production would
  compete for local cores with the warmup it is waiting on, and every order
  would be skipped as incomplete anyway.
* **One merge, not two.** The periodic refresh and a merge signal are handled in
  the same round, so a signal arriving alongside the timer makes the timer a
  no-op.
* **It does not force.** A merge signal yields `MergeAll(force=True, ...)`,
  whose `requires()` is every `MergePart`; during production those go incomplete
  again as soon as new jobs land, so Luigi's `check_unfulfilled_deps` raises
  `Unfulfilled dependency at run time: MergePart_...` between scheduling the
  task and running it -- observed on roughly half the hourly attempts of two
  live campaigns. The timer therefore yields `force=False`: `requires()` is
  empty, nothing races, and the snapshot is written from the part files as they
  stand, which is what a periodic view should be.

The clock is the newest `SIG_FINI` log entry, which the `finalize` CLI also
writes, so a manual finalize postpones the next automatic one. Clearing the log
on resubmit simply restarts the clock.

### Why `fini_tag` exists

Luigi never forgets a task it has run (`luigi_integration.md` §3), so a second
`MergeAll(force=True, finalize=True)` with identical parameters would be skipped
as already done and the timer would fire exactly once per submission.
`MergeAll.fini_tag` carries the request timestamp purely as task identity, and
`complete()` compares it against the tag recorded in `result/merge_all.json`:

```python
if self.fini_tag > float(marker.get("fini_tag", -1.0)):
    return False        # a newer request than the one already satisfied
```

An older request against a newer marker stays satisfied, so a resubmit does not
re-finalize needlessly.

### `complete()` had to stop trusting the flag alone

Writing `finalized: True` before the end of the run breaks an assumption the
finalize branch of `MergeAll.complete()` was making. It used to return as soon as
it had checked the marker's `run_tag`:

```python
if self.finalize:
    marker = self._read_merge_marker()
    ...
    return bool(marker.get("finalized", False))     # before the freshness checks
```

skipping the checks the ordinary branch performs — that the active parts still
match the marker, that no part carries the merge-in-progress sentinel, and that
no part has been merged since the marker was written. That was safe only while
nothing set `finalized` before the terminal merge.

With a timer setting it hourly, this sequence loses data:

1. a periodic finalize writes `result/final` and stamps `finalized: True`;
2. more jobs finish, and the ordinary `MergeAll` that `MergePart` yields folds
   them into `result/<obs>.dat` — the per-order files are now stale;
3. the run ends with everything merged, so every required `MergePart` reads
   complete;
4. `MergeFinal` asks for `MergeAll(finalize=True)`, which sees the stale
   `finalized` flag and reports complete.

The per-order files would then be missing the last batch of jobs while
`result/<obs>.dat` contains them — silently, since nothing failed.

The freshness checks are now factored into `_marker_is_current()` and applied to
**both** branches; `finalize` adds the `finalized` flag and the `fini_tag`
comparison on top of them, rather than in place of them. Whether the per-order
files are wanted is a separate question from whether the merge underneath them is
current, and the two are now asked separately.

### `MergeAll` is now mutually exclusive with itself

Every `MergeAll` writes the same `result/<obs>.dat`, and the finalizing ones also
the same `result/final/<order>.<obs>.dat`. They are distinct Luigi tasks whenever
their parameters differ — the plain one from `MergePart`, the forced one from a
merge signal, the periodic finalize — so nothing prevented two from running at
once and interleaving their writes. `MergeAll.resources` now claims

```python
{"MergeAll": 1}
```

An unregistered resource defaults to a limit of one in Luigi's scheduler
(`available_resources.get(resource, 1)`), so this acts as a global mutex without
needing to be declared in the scheduler config — the same idiom `MergePart` uses
for its per-part lock. This was a latent hazard before the timer existed; the
timer makes overlap likely enough to be worth closing.

## Cost

A finalize re-merges every part into every order it can write: orders x
observables invocations of the merge core, over all parts. It holds the
`MergeAll` mutex and one local core while it runs. Hourly is cheap against a
campaign measured in thousands of CPU-hours; setting it to minutes is not.
