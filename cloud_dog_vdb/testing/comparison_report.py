# Copyright 2026 Cloud-Dog, Viewdeck Engineering Limited
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from __future__ import annotations

import json
from pathlib import Path
from typing import Any


def render_comparison_markdown(report: dict[str, Any]) -> str:
    """Handle render comparison markdown."""
    lines: list[str] = []
    lines.append("# Cross-Provider Comparison Report")
    lines.append("")
    lines.append(f"- Generated at: {report.get('generated_at', '')}")
    lines.append("")
    lines.append("## Provider Summary")
    lines.append("")
    lines.append("| Provider | Total | OK | Success Ratio | Mean Parse Time (ms) | Quality Pass Rate |")
    lines.append("|---|---:|---:|---:|---:|---:|")
    summary = report.get("summary", {})
    if isinstance(summary, dict):
        for provider_id, values in sorted(summary.items()):
            if not isinstance(values, dict):
                continue
            lines.append(
                "| {provider} | {total} | {ok} | {ratio:.3f} | {latency:.2f} | {quality:.3f} |".format(
                    provider=provider_id,
                    total=int(values.get("total", 0) or 0),
                    ok=int(values.get("ok", 0) or 0),
                    ratio=float(values.get("success_ratio", 0.0) or 0.0),
                    latency=float(values.get("mean_parse_time_ms", 0.0) or 0.0),
                    quality=float(values.get("quality_invariant_pass_rate", 0.0) or 0.0),
                )
            )

    lines.append("")
    lines.append("## Cases")
    lines.append("")
    lines.append(
        "| Provider | Document ID | Category | Status | Mode | Text Chars | Tables | Images | Parse Time (ms) | Reason |"
    )
    lines.append("|---|---|---|---|---|---:|---:|---:|---:|---|")
    for case in report.get("cases", []):
        if not isinstance(case, dict):
            continue
        lines.append(
            "| {provider} | {doc} | {category} | {status} | {mode} | {text_chars} | {tables} | {images} | {latency:.2f} | {reason} |".format(
                provider=str(case.get("provider_id", "")),
                doc=str(case.get("document_id", "")),
                category=str(case.get("category", "")),
                status=str(case.get("status", "")),
                mode=str(case.get("execution_mode", "")),
                text_chars=int(case.get("text_chars", 0) or 0),
                tables=int(case.get("table_count", 0) or 0),
                images=int(case.get("image_count", 0) or 0),
                latency=float(case.get("parse_time_ms", 0.0) or 0.0),
                reason=str(case.get("reason", "")),
            )
        )
    lines.append("")
    return "\n".join(lines)


def write_comparison_report(
    report: dict[str, Any],
    *,
    output_dir: Path,
    report_basename: str,
) -> dict[str, Path]:
    """Handle write comparison report."""
    output_dir.mkdir(parents=True, exist_ok=True)
    json_path = output_dir / f"{report_basename}.json"
    markdown_path = output_dir / f"{report_basename}.md"
    json_path.write_text(json.dumps(report, indent=2), encoding="utf-8")
    markdown_path.write_text(render_comparison_markdown(report), encoding="utf-8")
    return {"json": json_path, "markdown": markdown_path}
