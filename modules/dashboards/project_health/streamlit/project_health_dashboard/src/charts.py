"""
Chart components for the CDF Project Health Dashboard.
"""

import plotly.graph_objects as go
from .config import COLORS


def _get_health_color(percentage: float) -> str:
    if percentage >= 90:
        return COLORS["success"]
    elif percentage >= 70:
        return "#FFBB33"
    elif percentage >= 50:
        return COLORS["warning"]
    else:
        return COLORS["failed"]


DONUT_COLORS = {
    "healthy": COLORS["success"],
    "ready": COLORS["success"],
    "completed": COLORS["success"],
    "failed": COLORS["failed"],
    "running": COLORS["pending"],
    "deploying": "#FFBB33",
    "unknown": COLORS["neutral"],
    "no_runs": COLORS["empty"],
    "healthy_calls": COLORS["success"],
    "failed_calls": COLORS["failed"],
    "no_calls": COLORS["empty"],
}


def create_health_gauge(healthy: int, total: int, title: str, icon: str = "") -> go.Figure:
    if total == 0:
        percentage = 100
        color = COLORS["neutral"]
    else:
        percentage = (healthy / total) * 100
        color = _get_health_color(percentage)
    fig = go.Figure(go.Indicator(
        mode="gauge",
        value=percentage,
        title={
            "text": f"{icon} {title}<br><span style='font-size:0.75em;color:gray'>{healthy}/{total} Healthy</span>",
            "font": {"size": 14},
        },
        gauge={
            "axis": {"range": [0, 100], "tickwidth": 1, "tickcolor": "#666666", "tickfont": {"size": 10}},
            "bar": {"color": color, "thickness": 0.7},
            "bgcolor": "white",
            "borderwidth": 2,
            "bordercolor": "#EEEEEE",
            "steps": [
                {"range": [0, 50], "color": "#FFEBEE"},
                {"range": [50, 70], "color": "#FFF3E0"},
                {"range": [70, 90], "color": "#FFFDE7"},
                {"range": [90, 100], "color": "#E8F5E9"},
            ],
            "threshold": {"line": {"color": "#333333", "width": 2}, "thickness": 0.75, "value": percentage},
        },
        domain={"x": [0.1, 0.9], "y": [0.25, 0.85]},
    ))
    fig.add_annotation(
        text=f"<b>{percentage:.0f}%</b>",
        x=0.5, y=0.35,
        xref="paper", yref="paper",
        showarrow=False,
        font=dict(size=28, color="#333333"),
        xanchor="center", yanchor="middle",
    )
    fig.update_layout(
        height=280,
        margin=dict(l=15, r=15, t=50, b=30),
        paper_bgcolor="rgba(0,0,0,0)",
        font={"color": "#333333", "family": "Arial"},
        autosize=True,
    )
    return fig


def create_status_donut(summary: dict, title: str, time_range_label: str = "") -> go.Figure:
    labels, values, colors = [], [], []
    for key, value in summary.items():
        if key == "total" or value <= 0:
            continue
        labels.append(key.replace("_", " ").title())
        values.append(value)
        colors.append(DONUT_COLORS.get(key, COLORS["neutral"]))
    if not values:
        labels, values, colors = ["No Data"], [1], [COLORS["empty"]]
    fig = go.Figure(data=[go.Pie(
        labels=labels,
        values=values,
        hole=0.6,
        marker_colors=colors,
        textinfo="none",
        hovertemplate="%{label}: %{value}<extra></extra>",
    )])
    title_text = f"{title}<br><span style='font-size:0.7em;color:gray'>📅 {time_range_label}</span>" if time_range_label else title
    fig.update_layout(
        title={"text": title_text, "x": 0.5, "xanchor": "center"},
        height=300,
        margin=dict(l=10, r=10, t=70, b=60),
        paper_bgcolor="rgba(0,0,0,0)",
        showlegend=True,
        legend=dict(orientation="h", yanchor="top", y=-0.05, xanchor="center", x=0.5, font=dict(size=11), itemsizing="constant"),
        autosize=True,
    )
    return fig
