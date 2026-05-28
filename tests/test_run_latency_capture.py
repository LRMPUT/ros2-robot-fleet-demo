"""Smoke tests for run_latency_capture.sh that need no Docker/broker."""
import os
import pathlib
import subprocess

REPO = pathlib.Path(__file__).resolve().parents[1]
SCRIPT = REPO / "tools" / "run_latency_capture.sh"


def test_script_passes_bash_syntax_check():
    r = subprocess.run(["bash", "-n", str(SCRIPT)], capture_output=True, text=True)
    assert r.returncode == 0, r.stderr


def test_missing_bag_path_fails_fast(tmp_path):
    # BAG_PATH points nowhere and no default bag exists under a temp CWD.
    env = dict(os.environ, BAG_PATH=str(tmp_path / "nope"), DURATION="1")
    r = subprocess.run(["bash", str(SCRIPT)], cwd=str(tmp_path),
                       env=env, capture_output=True, text=True)
    assert r.returncode != 0
    assert "metadata.yaml" in (r.stdout + r.stderr) or \
           "BAG" in (r.stdout + r.stderr)
