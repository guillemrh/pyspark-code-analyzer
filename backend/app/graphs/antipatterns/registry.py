from typing import Optional

from app.graphs.antipatterns.rules.multiple_actions import MultipleActionsRule
from app.graphs.antipatterns.rules.early_shuffle import EarlyShuffleRule
from app.graphs.antipatterns.rules.action_without_cache import ActionWithoutCacheRule
from app.graphs.antipatterns.rules.repartition_misuse import RepartitionMisuseRule
from app.graphs.antipatterns.rules.collect_on_large_data import CollectOnLargeDataRule
from app.graphs.antipatterns.rules.udf_usage import UDFUsageRule
from app.graphs.antipatterns.rules.broadcast_join_hint import BroadcastJoinHintRule
from app.graphs.antipatterns.rules.cartesian_join import CartesianJoinRule
from app.graphs.antipatterns.rules.collect_inside_loop import CollectInsideLoopRule
from app.graphs.antipatterns.rules.unpersist_missing import UnpersistMissingRule

RULES = [
    MultipleActionsRule(),
    EarlyShuffleRule(),
    ActionWithoutCacheRule(),
    RepartitionMisuseRule(),
    CollectOnLargeDataRule(),
    UDFUsageRule(),
    BroadcastJoinHintRule(),
    CartesianJoinRule(),
    CollectInsideLoopRule(),
    UnpersistMissingRule(),
]


def detect_antipatterns(dag, source_code: Optional[str] = None):
    """
    Detect anti-patterns in the operation DAG.

    Args:
        dag: The operation DAG to analyze
        source_code: Optional original source code for contextual suggestions

    Returns:
        List of AntiPatternFinding instances
    """
    findings = []
    for rule in RULES:
        findings.extend(rule.detect(dag, source_code))
    return findings
