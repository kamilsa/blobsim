#!/usr/bin/env python3
"""Execute and render the blobsim analysis notebook for a completed run.

A blobsim run writes to a single output directory (``shadow-output`` by default).
This script snapshots that finished run under a generated ``run_id``, executes
``notebooks/*.ipynb`` against it with papermill, converts the Plotly outputs to
self-contained HTML, and records the result in ``site/rendered/manifest.json`` so
the observatory site lists it.

Usage:
    python scripts/render_notebooks.py --run-dir shadow-output
    python scripts/render_notebooks.py --run-dir shadow-output --run-id my-run
"""

from __future__ import annotations

import argparse
import json
import sys
import tomllib
from datetime import datetime, timezone
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
NOTEBOOKS_DIR = REPO_ROOT / "notebooks"
SITE_RENDERED_DIR = REPO_ROOT / "site" / "rendered"
MANIFEST_PATH = SITE_RENDERED_DIR / "manifest.json"


def _load_manifest() -> dict:
    if MANIFEST_PATH.is_file():
        data = json.loads(MANIFEST_PATH.read_text())
        data.setdefault("runs", {})
        return data
    return {"runs": {}}


def _save_manifest(manifest: dict) -> None:
    MANIFEST_PATH.parent.mkdir(parents=True, exist_ok=True)
    MANIFEST_PATH.write_text(json.dumps(manifest, indent=2))


def _find_notebooks() -> list[Path]:
    if not NOTEBOOKS_DIR.is_dir():
        print(f"ERROR: {NOTEBOOKS_DIR} not found", file=sys.stderr)
        sys.exit(1)
    return sorted(NOTEBOOKS_DIR.glob("*.ipynb"))


def _generate_run_id() -> str:
    try:
        import coolname

        return coolname.generate_slug(3)
    except ImportError:
        return "run-" + datetime.now(timezone.utc).strftime("%Y%m%d-%H%M%S")


def _read_config() -> dict:
    try:
        with open(REPO_ROOT / "blobsim.toml", "rb") as f:
            return tomllib.load(f)
    except (OSError, tomllib.TOMLDecodeError):
        return {}


def _run_metadata(run_dir: Path) -> dict:
    """Build the manifest metadata card for this run from the config + host dirs."""
    cfg = _read_config()
    sim = cfg.get("sim", {})
    top = cfg.get("topology", {})
    # Per-run metadata (actual seed for this run) if the launcher wrote it.
    run_meta = {}
    rm_path = run_dir / "run-meta.json"
    if rm_path.is_file():
        try:
            run_meta = json.loads(rm_path.read_text())
        except (OSError, json.JSONDecodeError):
            run_meta = {}

    hosts_dir = run_dir / "shadow.data" / "hosts"
    hosts = [d.name for d in hosts_dir.iterdir() if d.is_dir()] if hosts_dir.is_dir() else []
    clients: dict[str, int] = {}
    for host in hosts:
        if host.startswith("spammer"):
            clients["blob-spammer"] = clients.get("blob-spammer", 0) + 1
        elif host == "proposer":
            clients["proposer/builder"] = clients.get("proposer/builder", 0) + 1
        elif host.startswith("validator"):
            clients["validator"] = clients.get("validator", 0) + 1

    slots = int(sim.get("slots", 0) or 0)
    return {
        "total_nodes": len(hosts),
        "clients": clients,
        "status": "complete",
        "duration_secs": slots * 12,
        "runner": cfg.get("run", {}).get("runner"),
        "seed": run_meta.get("seed", sim.get("seed")),
    }


# --- Plotly output handling (base64-numpy -> plain JSON, embed as div+script) ---

def _decode_numpy_binary(obj: dict) -> list:
    import base64
    import struct

    raw = base64.b64decode(obj["bdata"])
    dtype = obj.get("dtype", "f8")
    _MAP = {
        "b": ("b", 1), "B": ("B", 1), "h": ("h", 2), "H": ("H", 2),
        "i": ("i", 4), "I": ("I", 4), "l": ("l", 8), "L": ("L", 8),
        "f": ("f", 4), "d": ("d", 8), "e": ("e", 2),
        "i1": ("b", 1), "u1": ("B", 1), "i2": ("h", 2), "u2": ("H", 2),
        "i4": ("i", 4), "u4": ("I", 4), "i8": ("l", 8), "u8": ("L", 8),
        "f2": ("e", 2), "f4": ("f", 4), "f8": ("d", 8), "bool": ("b", 1),
    }
    if dtype[0] in ("<", ">", "|"):
        byte_order, core = dtype[0], dtype[1:]
    else:
        byte_order, core = "<", dtype
    fmt_char, item_size = _MAP.get(core, ("d", 8))
    count = len(raw) // item_size
    values = list(struct.unpack(f"{byte_order}{count}{fmt_char}", raw))

    shape_raw = obj.get("shape")
    if not shape_raw:
        return values
    if isinstance(shape_raw, str):
        shape = [int(p.strip()) for p in shape_raw.split(",") if p.strip()]
    else:
        shape = [int(p) for p in shape_raw]

    def _reshape(flat: list, dims: list[int]) -> list:
        if len(dims) <= 1:
            return flat[: dims[0]]
        step = 1
        for dim in dims[1:]:
            step *= dim
        return [_reshape(flat[i:i + step], dims[1:]) for i in range(0, dims[0] * step, step)]

    return _reshape(values, shape)


def _sanitize_plotly_json(obj):
    if isinstance(obj, dict):
        if "bdata" in obj and ("dtype" in obj or "shape" in obj or len(obj) <= 3):
            try:
                return _decode_numpy_binary(obj)
            except Exception:
                pass
        return {k: _sanitize_plotly_json(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [_sanitize_plotly_json(item) for item in obj]
    return obj


def _convert_plotly_outputs(nb) -> None:
    import uuid

    for cell in nb.cells:
        if cell.cell_type != "code":
            continue
        for output in cell.get("outputs", []):
            if output.get("output_type") not in ("display_data", "execute_result"):
                continue
            data = output.get("data", {})
            plotly_json = data.get("application/vnd.plotly.v1+json")
            if plotly_json:
                sanitized = _sanitize_plotly_json(plotly_json)
                div_id = f"plotly-{uuid.uuid4().hex[:8]}"
                data["text/html"] = (
                    f'<div id="{div_id}" class="plotly-graph-div" '
                    f'style="height:100%; width:100%;"></div>\n'
                    f'<script>Plotly.newPlot("{div_id}", {json.dumps(sanitized)});</script>'
                )


_CODE_TOGGLE_CSS = """<style>
.jp-InputArea, .input { display: none !important; }
.jp-OutputArea, .output_wrapper, .output { display: block !important; }
.code-toggle-btn {
    position: fixed; top: 12px; right: 16px; z-index: 9999;
    padding: 6px 14px; font-size: 12px; font-family: system-ui, sans-serif;
    background: var(--muted, #f0f0f0); color: var(--muted-foreground, #333);
    border: 1px solid var(--border, #ddd); border-radius: 6px; cursor: pointer;
    opacity: 0.7; transition: opacity 0.15s;
}
.code-toggle-btn:hover { opacity: 1; }
body.show-code .jp-InputArea, body.show-code .input { display: block !important; }
</style>
<script>
document.addEventListener('DOMContentLoaded', function() {
    var btn = document.createElement('button');
    btn.className = 'code-toggle-btn';
    btn.textContent = 'Show code';
    btn.onclick = function() {
        document.body.classList.toggle('show-code');
        btn.textContent = document.body.classList.contains('show-code') ? 'Hide code' : 'Show code';
    };
    document.body.appendChild(btn);
});
</script>"""

_PLOTLY_CDN = '<script src="https://cdn.plot.ly/plotly-latest.min.js"></script>'


def render_run(run_dir: Path, run_id: str | None = None) -> str | None:
    """Execute + render notebooks for one finished run. Returns the run_id, or None
    if the run is not finished (no host logs yet)."""
    import nbformat
    import papermill
    from nbconvert import HTMLExporter

    hosts_dir = run_dir / "shadow.data" / "hosts"
    if not hosts_dir.is_dir() or not any(hosts_dir.iterdir()):
        print(f"  SKIP: no host logs under {run_dir} (run not finished)", file=sys.stderr)
        return None

    run_id = run_id or _generate_run_id()
    print(f"  Rendering notebooks for run: {run_id}")

    output_dir = SITE_RENDERED_DIR / run_id
    output_dir.mkdir(parents=True, exist_ok=True)

    manifest = _load_manifest()
    rendered_notebooks = {}

    for notebook_path in _find_notebooks():
        notebook_id = notebook_path.stem
        print(f"    Executing {notebook_path.name}...")
        executed_path = output_dir / f"{notebook_id}.ipynb"
        try:
            papermill.execute_notebook(
                str(notebook_path),
                str(executed_path),
                parameters={"run_dir": str(run_dir.resolve()), "run_id": run_id},
                kernel_name="python3",
                request_save_on_cell_execute=False,
            )
        except papermill.PapermillExecutionError as e:
            print(f"    WARNING: execution error in {notebook_id}: {e}", file=sys.stderr)

        html_path = output_dir / f"{notebook_id}.html"
        with open(executed_path) as f:
            nb = nbformat.read(f, as_version=4)
        _convert_plotly_outputs(nb)

        exporter = HTMLExporter()
        exporter.template_name = "classic"
        body, _ = exporter.from_notebook_node(nb)

        if "Plotly.newPlot" in body:
            body = body.replace(
                '<div class="plotly-graph-div"',
                f'{_PLOTLY_CDN}\n{_CODE_TOGGLE_CSS}\n<div class="plotly-graph-div"',
                1,
            )
        else:
            body = body.replace("</body>", f"{_PLOTLY_CDN}\n{_CODE_TOGGLE_CSS}\n</body>", 1)

        html_path.write_text(body, encoding="utf-8")
        print(f"    -> {html_path}")
        executed_path.unlink(missing_ok=True)
        rendered_notebooks[notebook_id] = {"html_path": f"{run_id}/{notebook_id}.html"}

    manifest["runs"][run_id] = {
        "rendered_at": datetime.now(timezone.utc).isoformat(),
        "notebooks": rendered_notebooks,
        "metadata": _run_metadata(run_dir),
    }
    _save_manifest(manifest)
    print(f"  Updated manifest for {run_id}")
    return run_id


def main() -> None:
    parser = argparse.ArgumentParser(description="Render blobsim analysis notebooks")
    parser.add_argument("--run-dir", type=Path, default=Path("shadow-output"),
                        help="Finished run output directory (default: shadow-output)")
    parser.add_argument("--run-id", type=str, default=None,
                        help="Explicit run id (default: a generated slug)")
    args = parser.parse_args()

    run_dir = args.run_dir
    if not run_dir.is_absolute():
        run_dir = REPO_ROOT / run_dir
    render_run(run_dir.resolve(), args.run_id)


if __name__ == "__main__":
    main()
