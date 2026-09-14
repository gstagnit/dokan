# Luigi integration: the traps dokan keeps hitting

dokan drives Luigi in a way most Luigi users do not: one worker per active part,
a task process forked per attempt, dynamic dependencies everywhere, and a
`rich.Live` board owning the terminal. Each item below is a property of Luigi
that has cost real debugging time here, with what it means for dokan code. Read
this before adding a `Task` or a `Parameter`.

## 1. A parameter's `serialize()` and `parse()` must be symmetric

Luigi serializes a task's parameters to strings and reconstructs the task from
them. If `parse(serialize(x))` does not round-trip, the workflow dies with a
parse error far from the parameter that caused it.

The trap is an `IntEnum` with a custom `__str__`:

```python
class LogLevel(IntEnum):
    INFO = 20
    def __str__(self): return self.name.lower()   # -> "info"

log_level = luigi.OptionalIntParameter(default=LogLevel.INFO)
# serialize(LogLevel.INFO) == "info"      (str(x))
# parse("info")            -> ValueError: invalid literal for int() with base 10
```

It type-checks, it runs, and the value behaves as an int everywhere in Python.
Only the string round-trip is broken. `exe/_executor.py` therefore defines
`LogLevelParameter`, whose `serialize()` emits `str(int(x))`.

**Why it can hide for a long time** — see §2: Luigi serializes a dynamic
dependency *only when it is not already complete*. A task yielded for work whose
outputs are already on disk is resolved inline and never round-trips, so a
resumed run can stay up indefinitely while a fresh one dies as soon as the first
real work is dispatched.

**When adding a parameter**, assert the round-trip:

```python
p = MyParameter(...)
assert p.parse(p.serialize(value)) == value
```

## 2. A dynamic `yield` restarts `run()` from the top

`worker.TaskProcess._run_get_new_deps` drives the generator and, when a yielded
requirement is incomplete, returns it to the scheduler:

```python
if not requires.complete(self.check_complete):
    new_deps = [(t.task_module, t.task_family, t.to_str_params()) for t in ...]
    return new_deps
```

The task then goes back to PENDING and, once the dependency finishes, `run()` is
called **again from the beginning**, in a *new process*. Three consequences:

- **Nothing after the last executed `yield` runs in that invocation.** Cleanup
  placed after a `yield` executes only on the pass where that `yield` is not
  taken.
- **Instance attributes do not survive.** State that must persist across the
  suspension has to live on disk or in the database.
- **State written before a `yield` cannot distinguish "the dependency ran" from
  "the process died before it ran".** This is not a bug that can be fixed by
  writing a better fingerprint — the information is not available at that point.
  `MergePart`'s stall guard is the worked example; see `merge_failure_modes.md`.

Also note `run()` may be entered when the task is already complete (Luigi
re-checks), so `run()` should return early rather than redo work.

## 3. The worker never forgets a task

`worker.Worker` keeps `_scheduled_tasks` and `_add_task_history` for the lifetime
of the run and prunes neither, and the scheduler is no better: `update_status`
marks a task removable only once it has **no stakeholders**, and the single
long-lived worker is a stakeholder of everything it ever scheduled. Setting
`prune_on_get_work=True` therefore does nothing here — `prune()` runs and finds
nothing it may remove.

So **anything a parameter carries has a per-instance, permanent cost**, and
because Luigi forks a process per task attempt, the parent's accumulated size is
paid again by every fork.

### Sharing the value is not enough — share the *serialisation* too

`DictParameter.normalize()` deep-freezes its input on every instantiation, so a
configuration dict handed to every task is copied per instance. `SharedDictParameter`
in `task.py` returns a canonical frozen instance instead; frozen values are
immutable, so sharing is safe, and the value still compares equal to what
`DictParameter` produced.

That fixes the *dict* and leaves the bigger half of the problem. `Worker._add_task`
serialises a task's parameters and the scheduler keeps that dict for the task's
lifetime (`scheduler.Task.params`, plus its public/hidden views). `json.dumps`
returns a fresh string on every call, so every scheduled task pinned its own copy
of the configuration — measured at a 21 kB config, 2000 serialisations produced
2000 distinct strings.

`SharedDictParameter.serialize()` now memoises as well. End to end, adding tasks
through dokan's own worker/scheduler factory:

| | retained per task | distinct config strings (400 tasks) |
|---|---|---|
| before | 26.9 kB | 400 |
| after | **0.6 kB** | **1** |

A 98% cut: about 10 GB against 0.2 GB over 400k task instances. It is also ~60x
faster per call. This was the dominant term in a growth rate measured at ~0.8
GB/hour on two independent campaigns (5.41 GB at 6h44m, 8.40 GB at 10h36m).

### The task history is capped

`_add_task_history` gets one entry per status change of every task, each holding
a task reference, and is drained only by `luigi.execution_summary` at the end.
`WorkerSchedulerFactory.create_worker` replaces it with a bounded `deque`
(`_TASK_HISTORY_MAX`). A deque supports everything the summary does with it
(iteration and `[0]`), so the only consequence is that the summary describes the
recent tail rather than the whole run — and dokan reports through its own monitor
and log database anyway.

### What is still unbounded

`_scheduled_tasks` and the scheduler's own task table still grow with the number
of distinct tasks. With the payload no longer duplicated, what remains is the
Task objects themselves — small, but not nothing. Pruning them is possible in
principle (the worker reconstructs a missing task via `load_task`, so it is
designed to tolerate the scheduler forgetting), but it means reaching into Luigi
internals and re-parsing parameters, and it is not currently done.

**When adding a parameter that carries a payload** — a dict, a long list, a blob
— assume it will be retained once per task instance *and* serialised once per
task instance, and give it the same treatment.

## 4. Task failures reach only stderr, which is not readable here

Luigi reports failures through the `luigi-interface` logger. In a dokan run that
output is effectively invisible, for two independent reasons:

1. Every task attempt runs in its own forked process, while the `rich.Live`
   board lives only in the `Monitor` task's process. A worker's traceback races
   with the board's redraw on the shared terminal and is overwritten. No amount
   of nicer printing *from a worker* fixes this.
2. `luigi.build(..., log_level="WARNING")` suppresses Luigi's INFO notices,
   including `Task ... died unexpectedly with exit code ...`.

The log database is the only channel every forked process shares, and `Monitor`
already streams it to the board. `db/_dbtask.py` therefore registers three
handlers on the dokan `Task` base:

| Event | Fires when | Catches |
|---|---|---|
| `FAILURE` | the task raised | ordinary exceptions, with traceback |
| `PROCESS_FAILURE` | the worker died with no exception | hard kills: OOM, segfault |
| `BROKEN_TASK` | `complete()`/`requires()` raised | faults before any `run()` |

Two properties of that code are load-bearing:

- **The handler must never raise.** Luigi triggers `FAILURE` from inside its own
  `except` block (`TaskProcess._handle_run_exception`); an exception escaping the
  handler replaces the original failure and leaves the worker's result handling
  inconsistent. Everything is swallowed, with the console as a last-resort sink.
- **They are registered on the dokan `Task` base, not `DBTask`.** `MergeObs` is a
  plain `Task` — deliberately DB-free, so the standalone merge tool can use it —
  and its failures matter just as much.

**When adding a task that can fail in a way worth seeing, nothing extra is
needed** — but if it raises inside a `complete()` used by the scheduler, expect
`BROKEN_TASK` rather than `FAILURE`.

## 5. Luigi owns logging configuration, once

`setup_logging.BaseLogging.setup` is a one-shot: it sets `_configured` and
returns early on any later call. Its default handler configures the
`luigi-interface` logger with a single stderr handler **and sets the logger
level** to `log_level` — so INFO records are discarded before any handler sees
them, and attaching a `FileHandler` alone captures nothing.

`setup_luigi_logging()` in `__main__.py` therefore configures the logger itself
(logger at INFO, stderr at the console level, file at INFO) and marks
`InterfaceLogging._configured = True` so Luigi does not add a second stderr
handler on top.

It also drops its `FileHandler` in forked children via `os.register_at_fork`: an
inherited handler would give the run as many concurrent appenders as workers,
interleaving partial records once a traceback outgrows the stream buffer. Only
the parent writes, which loses nothing — child failures are already in the log
database (§4).

## 6. One worker per part means a lot of open files

dokan sizes its worker pool by the number of active parts, and every worker holds
its own SQLite connections and staged file handles, so a realistic process wants
file descriptors in the thousands while a login shell typically offers 1024.
`submit` therefore raises `RLIMIT_NOFILE` before `luigi.build`.

Only the **soft** limit, and only as far as the hard limit allows:

```python
soft, hard = resource.getrlimit(resource.RLIMIT_NOFILE)
if soft != resource.RLIM_INFINITY and soft < want:
    target = want if hard == resource.RLIM_INFINITY else min(want, hard)
    resource.setrlimit(resource.RLIMIT_NOFILE, (target, hard))
```

The soft limit is the process's own; anyone may raise it up to the hard limit.
Raising the *hard* limit needs `CAP_SYS_RESOURCE`, and `setrlimit` takes both
values in one call and rejects the call as a whole — so passing
`RLIM_INFINITY` as the hard value threw away the soft-limit change that was
wanted and permitted, and printed

```
failed to increase RLIMIT_NOFILE: not allowed to raise maximum limit
```

on every start, for an ordinary user. It was cosmetic — the run then proceeded on
the inherited limit, which on the systems where it was seen was already ample —
but it hid the case worth reporting: a hard limit genuinely too low for the
requested worker count. That case now warns with both numbers, and a system whose
limit is already sufficient says nothing at all.

## Checklist for a new Task or Parameter

- [ ] `parse(serialize(v)) == v` for every value the parameter can hold (§1)
- [ ] No payload in a parameter unless it can afford to be retained tens of
      thousands of times, or is shared (§3)
- [ ] `run()` returns early if the task is already complete (§2)
- [ ] No state written before a `yield` that is later interpreted as evidence
      the yielded work actually ran (§2)
- [ ] Anything worth seeing on failure goes to the log database, not stderr (§4)
- [ ] Per-worker resources (file handles, connections) scale with the pool, not
      with the process (§6)
