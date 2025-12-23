from enum import Enum

class OpType(str, Enum):
    TRANSFORMATION = "transformation"
    ACTION = "action"


# Canonical Spark operation registry
SPARK_OPS = {
    # Transformations
    "select": OpType.TRANSFORMATION,
    "filter": OpType.TRANSFORMATION,
    "withColumn": OpType.TRANSFORMATION,
    "groupBy": OpType.TRANSFORMATION,
    "join": OpType.TRANSFORMATION,
    "distinct": OpType.TRANSFORMATION,
    "drop": OpType.TRANSFORMATION,
    "repartition": OpType.TRANSFORMATION,
    "coalesce": OpType.TRANSFORMATION,

    # Actions
    "count": OpType.ACTION,
    "collect": OpType.ACTION,
    "show": OpType.ACTION,
    "take": OpType.ACTION,
    "foreach": OpType.ACTION,
    "write": OpType.ACTION,
}
