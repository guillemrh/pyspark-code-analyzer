from app.graphs.antipatterns.rules.multiple_actions import MultipleActionsRule
from app.graphs.antipatterns.rules.early_shuffle import EarlyShuffleRule
from app.graphs.antipatterns.rules.action_without_cache import ActionWithoutCacheRule
from app.graphs.antipatterns.rules.repartition_misuse import RepartitionMisuseRule
from app.graphs.antipatterns.rules.collect_on_large_data import CollectOnLargeDataRule
from app.graphs.antipatterns.rules.udf_usage import UDFUsageRule
from app.graphs.antipatterns.rules.broadcast_join_hint import BroadcastJoinHintRule

RULES = [
    MultipleActionsRule(),
    EarlyShuffleRule(),
    ActionWithoutCacheRule(),
    RepartitionMisuseRule(),
    CollectOnLargeDataRule(),
    UDFUsageRule(),
    BroadcastJoinHintRule(),
]


def detect_antipatterns(dag):
    findings = []
    for rule in RULES:
        findings.extend(rule.detect(dag))
    return findings
