"""Integration test: orchestrator must wire LATENCY_LOG_DIR into the compose file.

Stubs `docker` and `sleep` on PATH so the real run_latency_capture.sh + run.sh +
gen_fleet.sh execute without Docker/broker. The docker stub snapshots any
referenced fleet compose .yml during the `compose ... up` call (before the
teardown trap deletes it), so we can assert the latency mount was injected.
"""
import os
import pathlib
import stat
import subprocess

REPO = pathlib.Path(__file__).resolve().parents[1]
SCRIPT = REPO / "tools" / "run_latency_capture.sh"


def _make_stub(path, body):
    path.write_text("#!/usr/bin/env bash\n" + body)
    path.chmod(path.stat().st_mode | stat.S_IEXEC | stat.S_IXGRP | stat.S_IXOTH)


def test_orchestrator_injects_latency_mount(tmp_path):
    bindir = tmp_path / "bin"
    bindir.mkdir()
    capture = tmp_path / "captured"
    capture.mkdir()

    # docker stub: exit 0 for everything; on any call, snapshot referenced
    # fleet .yml files (those containing "robot_") into the capture dir; print a
    # fake container id for `run` (detached) so CONSUMER_CID is non-empty.
    _make_stub(bindir / "docker", f"""
set -e
for a in "$@"; do
  case "$a" in
    *.yml)
      if [[ -f "$a" ]] && grep -q "robot_" "$a" 2>/dev/null; then
        cp "$a" "{capture}/$(basename "$a").captured" 2>/dev/null || true
      fi
      ;;
  esac
done
if [[ "$1" == "run" ]]; then echo "fakecid123"; fi
exit 0
""")
    # sleep stub: return immediately so the DURATION/bringup waits don't block.
    _make_stub(bindir / "sleep", "exit 0\n")

    # Fake converted bag (only metadata.yaml is checked by the scripts).
    bag = tmp_path / "bag"
    bag.mkdir()
    (bag / "metadata.yaml").write_text("rosbag2_bagfile_information:\n")

    out = tmp_path / "out"
    env = dict(os.environ)
    env["PATH"] = f"{bindir}:{env['PATH']}"
    env.update(N="1", BROKER="kafka", DURATION="1",
               BAG_PATH=str(bag), OUTPUT_DIR=str(out))

    r = subprocess.run(["bash", str(SCRIPT)], env=env,
                       capture_output=True, text=True, timeout=120)
    assert r.returncode == 0, f"stdout:\n{r.stdout}\nstderr:\n{r.stderr}"

    captured = list(capture.glob("*.captured"))
    assert captured, f"no fleet compose captured; stdout:\n{r.stdout}\nstderr:\n{r.stderr}"
    combined = "\n".join(p.read_text() for p in captured)
    assert "${LATENCY_LOG_DIR}:/latency" in combined, \
        f"latency mount missing from generated compose:\n{combined}"
    assert 'LATENCY_LOG_DIR: "/latency"' in combined
