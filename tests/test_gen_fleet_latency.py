"""gen_fleet.sh injects latency log mount only when LATENCY_LOG_DIR is set."""
import os
import pathlib
import subprocess

REPO = pathlib.Path(__file__).resolve().parents[1]
GEN = REPO / "gen_fleet.sh"


def _run(out_file, env_extra):
    env = dict(os.environ, BROKER="kafka", MSG_TYPE="multi", **env_extra)
    env.pop("LATENCY_LOG_DIR", None)
    env.update(env_extra)
    subprocess.run(["bash", str(GEN), "2", str(out_file)],
                   check=True, env=env, capture_output=True, text=True)
    return out_file.read_text()


def test_no_injection_when_unset(tmp_path):
    out = tmp_path / "fleet.yml"
    text = _run(out, {})
    assert "LATENCY_LOG_DIR" not in text
    assert ":/latency" not in text


def test_injection_when_set(tmp_path):
    out = tmp_path / "fleet.yml"
    text = _run(out, {"LATENCY_LOG_DIR": "/host/logs"})
    # Env var present for both robots.
    assert text.count('LATENCY_LOG_DIR: "/latency"') == 2
    # Volume mount references the compose-substituted host path var.
    assert text.count("${LATENCY_LOG_DIR}:/latency") == 2
