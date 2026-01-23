from enum import Enum


class OpType(str, Enum):
    TRANSFORMATION = "transformation"
    ACTION = "action"


class DependencyType(str, Enum):
    NARROW = "narrow"
    WIDE = "wide"


SHUFFLE_OPS = {
    "groupBy",
    "join",
    "distinct",
    "repartition",
    "orderBy",
    "sort",
}

# Data source operations (spark.read.*)
DATA_SOURCE_OPS = {
    "parquet",
    "csv",
    "json",
    "orc",
    "avro",
    "table",
    "jdbc",
    "text",
    "format",
    "load",
}

# Canonical Spark operation registry
SPARK_OPS = {
    # Data Source Operations (spark.read.*)
    "parquet": OpType.TRANSFORMATION,
    "csv": OpType.TRANSFORMATION,
    "json": OpType.TRANSFORMATION,
    "orc": OpType.TRANSFORMATION,
    "avro": OpType.TRANSFORMATION,
    "table": OpType.TRANSFORMATION,
    "jdbc": OpType.TRANSFORMATION,
    "text": OpType.TRANSFORMATION,
    "format": OpType.TRANSFORMATION,
    "load": OpType.TRANSFORMATION,
    # SQL
    "sql": OpType.TRANSFORMATION,
    # Transformations - Selection/Projection
    "select": OpType.TRANSFORMATION,
    "selectExpr": OpType.TRANSFORMATION,
    "filter": OpType.TRANSFORMATION,
    "where": OpType.TRANSFORMATION,
    "withColumn": OpType.TRANSFORMATION,
    "withColumns": OpType.TRANSFORMATION,
    "withColumnRenamed": OpType.TRANSFORMATION,
    "drop": OpType.TRANSFORMATION,
    "dropDuplicates": OpType.TRANSFORMATION,
    "dropDuplicatesWithinWatermark": OpType.TRANSFORMATION,
    "alias": OpType.TRANSFORMATION,
    "toDF": OpType.TRANSFORMATION,
    # Transformations - Aggregation
    "groupBy": OpType.TRANSFORMATION,
    "agg": OpType.TRANSFORMATION,
    "aggregate": OpType.TRANSFORMATION,
    "rollup": OpType.TRANSFORMATION,
    "cube": OpType.TRANSFORMATION,
    # Transformations - Sorting
    "orderBy": OpType.TRANSFORMATION,
    "sort": OpType.TRANSFORMATION,
    "sortWithinPartitions": OpType.TRANSFORMATION,
    # Transformations - Joins
    "join": OpType.TRANSFORMATION,
    "crossJoin": OpType.TRANSFORMATION,
    # Transformations - Set Operations
    "union": OpType.TRANSFORMATION,
    "unionAll": OpType.TRANSFORMATION,
    "unionByName": OpType.TRANSFORMATION,
    "intersect": OpType.TRANSFORMATION,
    "intersectAll": OpType.TRANSFORMATION,
    "subtract": OpType.TRANSFORMATION,
    "exceptAll": OpType.TRANSFORMATION,
    # Transformations - Sampling/Limiting
    "limit": OpType.TRANSFORMATION,
    "sample": OpType.TRANSFORMATION,
    "sampleBy": OpType.TRANSFORMATION,
    # Transformations - Null Handling
    "fillna": OpType.TRANSFORMATION,
    "fill": OpType.TRANSFORMATION,
    "dropna": OpType.TRANSFORMATION,
    "na": OpType.TRANSFORMATION,
    "replace": OpType.TRANSFORMATION,
    # Transformations - Partitioning
    "distinct": OpType.TRANSFORMATION,
    "repartition": OpType.TRANSFORMATION,
    "repartitionByRange": OpType.TRANSFORMATION,
    "coalesce": OpType.TRANSFORMATION,
    # Transformations - Caching
    "cache": OpType.TRANSFORMATION,
    "persist": OpType.TRANSFORMATION,
    "unpersist": OpType.TRANSFORMATION,
    # Transformations - Checkpointing
    "checkpoint": OpType.TRANSFORMATION,
    "localCheckpoint": OpType.TRANSFORMATION,
    # Transformations - Miscellaneous
    "transform": OpType.TRANSFORMATION,
    "hint": OpType.TRANSFORMATION,
    "withWatermark": OpType.TRANSFORMATION,
    "withMetadata": OpType.TRANSFORMATION,
    # Actions - Retrieval
    "count": OpType.ACTION,
    "collect": OpType.ACTION,
    "first": OpType.ACTION,
    "head": OpType.ACTION,
    "take": OpType.ACTION,
    "tail": OpType.ACTION,
    "toLocalIterator": OpType.ACTION,
    # Actions - Display/Debug
    "show": OpType.ACTION,
    "describe": OpType.ACTION,
    "summary": OpType.ACTION,
    "explain": OpType.ACTION,
    "printSchema": OpType.ACTION,
    # Actions - Iteration
    "foreach": OpType.ACTION,
    "foreachPartition": OpType.ACTION,
    # Actions - Conversion
    "toPandas": OpType.ACTION,
    "toArrow": OpType.ACTION,
    # Actions - Write
    "write": OpType.ACTION,
    "writeStream": OpType.ACTION,
    "save": OpType.ACTION,
}
