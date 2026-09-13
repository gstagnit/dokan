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
of the run and prunes neither. Every task instance ever scheduled is retained,
and dokan creates one instance per dynamic clone — tens of thousands over a long
run.

This makes **anything a parameter carries a per-instance, permanent cost**, and
because Luigi forks a process per task attempt, the parent's accumulated size is
paid again by every fork.

`DictParameter.normalize()` deep-freezes its input on *every* instantiation, so
a configuration dict handed to every task used to be copied per instance. Hence
`SharedDictParameter` in `task.py`, which returns a canonical frozen instance —
frozen values are immutable, so sharing is safe, and the value still compares
equal to what `DictParameter` produced (task ids and the `to_str_params()`
round-trip are unaffected).

**When adding a parameter that carries a payload** — a dict, a long list, a
blob — assume it will be retained a few tens of thousands of times.

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

## Checklist for a new Task or Parameter

- [ ] `parse(serialize(v)) == v` for every value the parameter can hold (§1)
- [ ] No payload in a parameter unless it can afford to be retained tens of
      thousands of times, or is shared (§3)
- [ ] `run()` returns early if the task is already complete (§2)
- [ ] No state written before a `yield` that is later interpreted as evidence
      the yielded work actually ran (§2)
- [ ] Anything worth seeing on failure goes to the log database, not stderr (§4)
