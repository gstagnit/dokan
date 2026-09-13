# Reading the monitor

What the live status board and the log stream actually tell you, including the
things that look like faults and are not.

## The board

Columns are contributions (`LO`, `R`, `V`, `RRa`, `RRb`, `RV`, `VV`), ordered by
perturbative order then alphabetically. Rows are `part_num`. Each cell is one
part:

```
PRD A[3/5] D[12] F[1]
 |     |     |     `- FAILED
 |     |     `------- successful (DONE + MERGED), all run tags
 |     `------------- active: running in this run tag / total active
 `------------------- WRM (warmup) or PRD (production)
```

`WRM` appears while the part has any active warmup job, otherwise `PRD`. The
mode label is bold while something of this run tag is actually running and dim
otherwise, and `F[...]` is only shown when non-zero. The counts other than the
running split are totals across run tags, so they carry over from earlier
submissions.

### Why some row numbers are missing

A `-` cell is not missing data: it means that contribution has no channel for
that initial state.

`part_num` is a **shared index into the canonical initial-state list**, not a
per-contribution counter. The same `part_num` denotes the same partonic initial
state in every contribution, so a contribution lacking that initial state leaves
a hole, and the table is sized by the largest `part_num` present:

```
   part_num        RV           VV            V
       11      [31,41]      [31,41]      [31,41]
       12     [-30,30]           --           --
       13    [-31,-21]    [-31,-21]    [-31,-21]
       14     [30,-30]           --           --
```

Contributions at lower order typically have fewer channels, so their columns are
the gappy ones. To confirm what a given row is, read the channel definitions out
of the run's `config.json` under `process.channels` — each carries its
`part_num` and its `! channel: [a,b]` label.

### Layout

When the terminal is wide enough the board and the log sit side by side, the log
in a panel to the right of the board. Otherwise the board is drawn alone and log
records are printed above it, as they always were.

The split exists because the board is as tall as the process has channels — for a
realistic process that is most of the terminal, leaving a couple of lines above
it in which a message appears and is immediately pushed out of sight. In the
panel a whole screenful stays put.

Two consequences worth knowing:

* the panel shows **one line per record**, elided rather than wrapped, so the
  number of visible records is predictable — a long message is cut, not spread
  over four lines;
* records in the panel are *not* in the terminal's scrollback, since they live
  inside the live region. Nothing is lost: every record is in `log.sqlite`, which
  is the place to read history from anyway (see the queries below).

The fallback triggers below `board width + 60` columns; a cramped panel is worse
than none.

## The log stream

Messages are written to the run's log database and streamed above the board.
Each names the task and the part it concerns.

| Message | Meaning |
|---|---|
| `PreProduction[<part>]::run:  next warmup step: N seed(s) x M[K]` | the next warmup step: `N` parallel seeds of `M` events over `K` iterations |
| `DBDispatch[<id>]::run:  dispatched N job(s) in M batch(es)` | jobs handed to runners; the idle case is not logged |
| `DBRunner[<part>]::run:  batch <seeds>: N job(s) <status>` | state of one batch the runner is driving |
| `DBRunner[<part>]::run:  N job(s) finished -> merging part` | the batch completed and triggered a merge |
| `MergePart[<part>]::run:  merging N new job(s)` | a fresh merge of that part |
| `MergePart[<part>]::run:  continuing in-progress merge of N job(s)` | the in-progress sentinel is set — **normal**, see below |
| `MergePart[<part>]::run:  no accepted events (cross = 0 +/- 0)` | the part contributes nothing, see below |
| `MergeAll[...]::run` | the combined result is being rebuilt |
| `Entry::run:  complete MergeAll -> dispatch` | pre-production is done; production begins |

### "continuing in-progress merge" is not an error

`MergePart` marks a part in-progress before yielding its per-observable merges,
and Luigi restarts `run()` from the top once those finish (see
`luigi_integration.md` §2). The second pass is what finalises the part, so this
message appears on **every** healthy merge, normally paired with the "merging N
new job(s)" line that precedes it. It also appears after a crash mid-merge, and
the two cases are indistinguishable from the sentinel alone — which is exactly
why the wording does not claim a fault.

### "no accepted events" and why that part then goes quiet

Printed when a part's `cross` comes out as exactly `0 +/- 0`: no event passed the
selection. This is an expected outcome for a partonic channel that cannot
contribute to the requested observable, and it does not improve with statistics
— such a part stays at zero however many events it is given.

The consequence is worth knowing: the part carries error 0, and
`_distribute_jobs` filters on `error > 0.0`, so it is excluded from the error
budget and receives no further production jobs. That is the intended behaviour —
no CPU is spent on channels that cannot contribute — but if a part you *expect*
to contribute reports this, the selection or the channel definition is worth a
look rather than the merge.

The relative error is floored at a small non-zero value purely so the optimiser
does not divide by zero.

### Executor log echoes

`DBRunner` echoes the executor's own log for the job it is reporting on, and
only entries at or after that job's start. Older entries are suppressed: the
grid state handed from a warmup step to later jobs used to bring the warmup's
log along with it, so a grid-adaptation line could resurface in a production
directory and read as if grids were still being adapted there. Anything echoed
now belongs to the job named in the message.

## When something looks stuck

The board keeps refreshing and the process keeps running even when no work can
proceed — a stalled workflow looks like a slow one. The tell is a gap of almost
exactly Luigi's `retry_delay` (900 s by default) between bursts of log activity.

`merge_failure_modes.md` covers the diagnosis: what to check, in what order, and
the failure modes that produce it.
