"""Shared Plotly theming + small stats helpers for the blobsim analysis notebook.

Kept dependency-light (plotly + pandas + numpy) so it imports cleanly under the
papermill render pipeline. The palette and helper signatures are the contract the
notebook cells rely on.
"""

from __future__ import annotations

import numpy as np
import pandas as pd
import plotly.graph_objects as go

# Flat-UI-ish categorical palette. Indexed directly (e.g. [0], [4], [6], [8]) and
# as ``[i % len]`` by the notebook, and reused as heatmap colour stops, so keep it
# ordered light→dark enough that mid indices read as mid-intensity.
FLAT_UI_PALETTE: list[str] = [
    "#2980b9",  # 0 belize blue
    "#27ae60",  # 1 nephritis green
    "#c0392b",  # 2 pomegranate red
    "#8e44ad",  # 3 wisteria purple
    "#f39c12",  # 4 orange
    "#16a085",  # 5 green sea
    "#d35400",  # 6 pumpkin
    "#2c3e50",  # 7 midnight blue
    "#7f8c8d",  # 8 concrete grey
    "#e74c3c",  # 9 alizarin
    "#1abc9c",  # 10 turquoise
    "#34495e",  # 11 wet asphalt
]


def apply_theme(fig: go.Figure) -> go.Figure:
    """Apply the shared look to ``fig`` in place and return it for chaining."""
    fig.update_layout(
        template="plotly_white",
        font=dict(family="Inter, system-ui, sans-serif", size=13, color="#2c3e50"),
        title=dict(font=dict(size=16)),
        margin=dict(l=60, r=30, t=60, b=50),
        legend=dict(bgcolor="rgba(255,255,255,0.6)", borderwidth=0),
        colorway=FLAT_UI_PALETTE,
        hoverlabel=dict(font_size=12),
    )
    fig.update_xaxes(showgrid=True, gridcolor="rgba(44,62,80,0.08)", zeroline=False)
    fig.update_yaxes(showgrid=True, gridcolor="rgba(44,62,80,0.08)", zeroline=False)
    return fig


def ecdf_trace(
    series: pd.Series | np.ndarray,
    name: str,
    *,
    denominator: int | None = None,
    line: dict | None = None,
) -> go.Scatter:
    """A right-censorable step ECDF trace.

    ``series`` may contain NaN (censored / never-observed); those are dropped from
    the curve but still counted when ``denominator`` is the full eligible
    population — so the curve plateaus below 1.0, exposing the censored fraction.
    """
    values = pd.to_numeric(pd.Series(series), errors="coerce").dropna()
    values = np.sort(values.to_numpy(dtype=float))
    n = len(values)
    denom = denominator if denominator else n
    if n == 0 or denom == 0:
        y = np.array([], dtype=float)
    else:
        y = np.arange(1, n + 1, dtype=float) / float(denom)
    return go.Scatter(
        x=values,
        y=y,
        name=name,
        mode="lines",
        line_shape="hv",
        line=line or {},
    )


def percentile_summary(df: pd.DataFrame, group: str, value: str) -> pd.DataFrame:
    """Per-group p50/p90/p95 of ``value`` plus eligible/completed counts.

    Censoring-aware: ``eligible`` counts every row in the group, ``completed``
    counts non-NaN ``value`` rows, and percentiles are computed over completed
    observations only. The returned frame keeps the grouping column named exactly
    as ``group`` (callers reference it directly, e.g. ``slot_num``/``role_group``).
    """
    cols = [group, "p50", "p90", "p95", "eligible", "completed", "completion_fraction"]
    if df is None or df.empty or group not in df or value not in df:
        return pd.DataFrame(columns=cols)

    work = df[[group, value]].copy()
    work[value] = pd.to_numeric(work[value], errors="coerce")

    rows = []
    for key, grp in work.groupby(group, dropna=False, sort=True):
        vals = grp[value].dropna()
        eligible = int(len(grp))
        completed = int(len(vals))
        rows.append(
            {
                group: key,
                "p50": float(np.percentile(vals, 50)) if completed else np.nan,
                "p90": float(np.percentile(vals, 90)) if completed else np.nan,
                "p95": float(np.percentile(vals, 95)) if completed else np.nan,
                "eligible": eligible,
                "completed": completed,
                "completion_fraction": completed / eligible if eligible else np.nan,
            }
        )
    return pd.DataFrame(rows, columns=cols)
