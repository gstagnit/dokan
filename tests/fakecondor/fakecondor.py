#!/usr/bin/env python3
"""A stand-in for the HTCondor command-line tools, for testing detached execution.

The four commands dokan uses (`condor_submit`, `condor_q`, `condor_release`,
`condor_config_val`) are symlinks to this file; the queue lives in the JSON file
named by `$FAKE_CONDOR_STATE`.  Tests mutate that file directly to make jobs
"finish" (see `FakeCondor` in `tests/test_tick.py`).

Only the syntax dokan actually uses is understood.  Plain Python 3.9, standard
library only: the shims run under whatever `python3` is first on the PATH.
"""

import fcntl
import json
import os
import re
import sys

SCHEDD = "fakeschedd.example.org"


def _state_path():
    path = os.environ.get("FAKE_CONDOR_STATE")
    if not path:
        sys.exit("FAKE_CONDOR_STATE is not set")
    return path


def _load(handle):
    handle.seek(0)
    raw = handle.read()
    if not raw.strip():
        return {"next_cluster": 1000, "clusters": {}, "calls": []}
    return json.loads(raw)


def _store(handle, state):
    handle.seek(0)
    handle.truncate()
    json.dump(state, handle, indent=1)
    handle.flush()


def _with_state(fn):
    """Run `fn(state) -> result` under an exclusive lock on the state file."""
    with open(_state_path(), "a+") as handle:
        fcntl.flock(handle, fcntl.LOCK_EX)
        state = _load(handle)
        result = fn(state)
        _store(handle, state)
    return result


def condor_submit(argv):
    sub_file = argv[0] if argv else "job.sub"
    with open(sub_file) as f:
        text = f.read()
    queue = re.search(r"^\s*queue\s+(\d+)", text, re.MULTILINE)
    initialdir = re.search(r"^\s*initialdir\s*=\s*(\S+)", text, re.MULTILINE)
    start_seed = re.search(r"\[ProcId\+(\d+)\]", text)
    nseed = int(queue.group(1)) if queue else 1
    iwd = initialdir.group(1) if initialdir else os.getcwd()
    if os.environ.get("FAKE_CONDOR_SUBMIT_FAILS"):
        sys.stderr.write("ERROR: fake schedd refuses submissions\n")
        return 1

    def submit(state):
        cluster = state["next_cluster"]
        state["next_cluster"] = cluster + 1
        state["clusters"][str(cluster)] = {
            "iwd": iwd,
            "start_seed": int(start_seed.group(1)) if start_seed else 1,
            "procs": {str(proc): 1 for proc in range(nseed)},  # 1 = Idle
        }
        state["calls"].append(["condor_submit", iwd, nseed])
        return cluster

    cluster = _with_state(submit)
    sys.stdout.write(f"Submitting job(s){'.' * nseed}\n{nseed} job(s) submitted to cluster {cluster}.\n")
    return 0


def condor_q(argv):
    """`condor_q [-name S | -global] -nobatch -af:t GlobalJobId JobStatus Iwd`."""
    if os.environ.get("FAKE_CONDOR_Q_FAILS"):
        sys.stderr.write("Error: fake schedd unreachable\n")
        return 1
    name = None
    if "-name" in argv:
        name = argv[argv.index("-name") + 1]
        if name != SCHEDD:
            sys.stderr.write(f"Error: unknown schedd {name}\n")
            return 1

    def query(state):
        state["calls"].append(["condor_q", " ".join(argv)])
        rows = []
        for cluster, info in state["clusters"].items():
            for proc, status in info["procs"].items():
                rows.append(f"{SCHEDD}#{cluster}.{proc}#1700000000\t{status}\t{info['iwd']}")
        return rows

    for row in _with_state(query):
        sys.stdout.write(row + "\n")
    return 0


def condor_release(argv):
    cluster = argv[-1]

    def release(state):
        info = state["clusters"].get(cluster)
        if info is None:
            return 1
        for proc, status in info["procs"].items():
            if status == 5:
                info["procs"][proc] = 1
        state["calls"].append(["condor_release", cluster])
        return 0

    return _with_state(release)


def condor_config_val(argv):
    if argv and argv[0] == "SCHEDD_HOST":
        sys.stdout.write(SCHEDD + "\n")
        return 0
    sys.stderr.write("Not defined\n")
    return 1


def main():
    name = os.path.basename(sys.argv[0])
    commands = {
        "condor_submit": condor_submit,
        "condor_q": condor_q,
        "condor_release": condor_release,
        "condor_config_val": condor_config_val,
    }
    if name not in commands:
        sys.exit(f"fakecondor: unknown command {name}")
    sys.exit(commands[name](sys.argv[1:]))


if __name__ == "__main__":
    main()
