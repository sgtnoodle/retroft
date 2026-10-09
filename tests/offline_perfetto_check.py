#!/usr/bin/env python3
"""Validate native imports with an explicitly supplied official Perfetto binary.

Generate inputs first:
  RETROFT_TEST_ARTIFACT_DIR=/tmp/offline-artifacts cargo test -p retroft-export --lib native_perfetto_export
Then run:
  python3 tests/offline_perfetto_check.py /path/to/trace_processor_shell /tmp/offline-artifacts
No downloads, target instrumentation, or Perfetto installation are performed.
"""
import csv
import io
import subprocess
import sys
from pathlib import Path

binary, directory = sys.argv[1:]

def query(trace, sql):
    result = subprocess.run([binary, "query", str(trace), sql], check=True,
                            text=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    return list(csv.DictReader(io.StringIO(result.stdout.strip())))

for name in ["little64", "big32"]:
    trace = Path(directory) / (name + ".pftrace")
    rows = query(trace, "SELECT sched.ts,sched.dur,sched.cpu,thread.tid,thread.name "
                 "FROM sched JOIN thread USING(utid) ORDER BY sched.ts")
    assert rows == [
        {"ts":"1510", "dur":"268436457", "cpu":"0", "tid":"42", "name":"worker"},
        {"ts":"268437967", "dur":"7", "cpu":"0", "tid":"43", "name":"worker-child"},
        {"ts":"268437974", "dur":"-1", "cpu":"0", "tid":"0", "name":"swapper"},
    ], rows
    rows = query(trace, "SELECT ts,dur,state FROM thread_state JOIN thread USING(utid) "
                 "WHERE tid=43 ORDER BY ts")
    assert rows[0] == {"ts":"134219248", "dur":"134218719", "state":"R"}, rows
    assert query(trace, "SELECT ts,name FROM slice") == [
        {"ts":"2500", "name":"Trigger 7: fixture fault"}]
    assert query(trace, "SELECT name,value FROM stats WHERE severity='error' AND value>0") == []
    assert query(trace, "SELECT value FROM stats WHERE name='ftrace_cpu_has_data_loss'") == [{"value":"1"}]
    print(name, "scheduling, names, waking latency, trigger, loss: verified")
