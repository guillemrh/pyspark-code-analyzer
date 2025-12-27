from app.graphs.antipatterns.rules.multiple_actions import MultipleActionsRule
from app.graphs.antipatterns.rules.early_shuffle import EarlyShuffleRule
from app.graphs.antipatterns.rules.action_without_cache import ActionWithoutCacheRule
from app.graphs.antipatterns.rules.repartition_misuse import RepartitionMisuseRule

RULES = [
    MultipleActionsRule(),
    EarlyShuffleRule(),
    ActionWithoutCacheRule(),
    RepartitionMisuseRule(),
]


def detect_antipatterns(dag):
    findings = []
    for rule in RULES:
        findings.extend(rule.detect(dag))
    return findings
