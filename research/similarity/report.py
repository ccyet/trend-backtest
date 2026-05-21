from __future__ import annotations

from io import BytesIO

import pandas as pd
import plotly.express as px


def cases_to_csv_bytes(cases: pd.DataFrame) -> bytes:
    return cases.to_csv(index=False).encode("utf-8-sig")


def build_html_report_bytes(
    *,
    title: str,
    status: str,
    message: str,
    cases: pd.DataFrame,
) -> bytes:
    html = [
        "<html><head><meta charset='utf-8'><title>",
        title,
        "</title></head><body>",
        f"<h1>{title}</h1>",
        f"<p><strong>策略状态：</strong>{status}</p>",
        f"<p>{message}</p>",
        cases.to_html(index=False) if not cases.empty else "<p>暂无有效样本。</p>",
        "</body></html>",
    ]
    return "".join(html).encode("utf-8")


def cases_to_excel_bytes(cases: pd.DataFrame) -> bytes:
    output = BytesIO()
    with pd.ExcelWriter(output, engine="openpyxl") as writer:
        cases.to_excel(writer, sheet_name="similar_cases", index=False)
    return output.getvalue()


def current_path_png_bytes(current_window: pd.DataFrame) -> bytes:
    if current_window.empty:
        return b""
    frame = current_window.copy()
    frame["normalized_close"] = (
        frame["close"].astype(float) / float(frame["close"].iloc[0]) * 100
    )
    fig = px.line(frame, x="date", y="normalized_close", title="当前窗口归一化路径")
    return fig.to_image(format="png")
