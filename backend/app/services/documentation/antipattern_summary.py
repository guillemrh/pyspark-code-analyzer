from typing import Dict, Any, List
from collections import defaultdict

from app.graphs.antipatterns.base import AntiPatternFinding


def build_antipattern_summary(
    findings: List[AntiPatternFinding],
) -> Dict[str, Any]:
    """
    Aggregate detected anti-patterns into a summary structure.
    """

    by_rule = defaultdict(list)
    by_severity = defaultdict(int)

    for f in findings:
        by_rule[f.rule_id].append(
            {
                "severity": f.severity,
                "message": f.message,
                "nodes": f.nodes,
            }
        )
        by_severity[f.severity] += 1

    return {
        "total_issues": len(findings),
        "by_severity": dict(by_severity),
        "by_rule": dict(by_rule),
    }


def antipattern_summary_markdown(summary: Dict[str, Any]) -> str:
    """
    Render anti-pattern summary as Markdown.
    """

    lines = [
        "## Performance Anti-Patterns",
        f"**Total issues detected:** {summary['total_issues']}",
        "",
    ]

    if summary["total_issues"] == 0:
        lines.append("✅ No performance anti-patterns detected.")
        return "\n".join(lines)

    lines.append("### Issues by severity")
    for severity, count in summary["by_severity"].items():
        lines.append(f"- **{severity}**: {count}")
    lines.append("")

    for rule_id, issues in summary["by_rule"].items():
        lines.append(f"### {rule_id}")
        for issue in issues:
            lines.append(
                f"- **{issue['severity']}**: {issue['message']} "
                f"(nodes: {', '.join(issue['nodes'])})"
            )
        lines.append("")

    return "\n".join(lines)
