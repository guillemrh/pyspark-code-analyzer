from typing import Dict, Any
from app.graphs.lineage.lineage_graph_builder import DataLineageGraph


def lineage_summary_json(lineage: DataLineageGraph) -> Dict[str, Any]:
    return {
        "datasets": [
            {
                "dataset": df,
                "depends_on": sorted(list(parents)),
            }
            for df, parents in lineage.parents.items()
        ]
    }


def lineage_summary_markdown(lineage: DataLineageGraph) -> str:
    lines = ["## Data Lineage\n"]

    for df, parents in lineage.parents.items():
        if parents:
            lines.append(f"- **{df}** ← {', '.join(sorted(parents))}")
        else:
            lines.append(f"- **{df}** (source)")

    return "\n".join(lines)
