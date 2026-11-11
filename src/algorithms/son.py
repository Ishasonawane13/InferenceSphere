"""SON frequent-itemset mining over source users' outgoing neighborhoods."""

from __future__ import annotations

import json
import logging
import math
import time
from itertools import combinations
from pathlib import Path
from typing import Iterable, Iterator, Sequence

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col

from src.config import AppConfig, load_config
from src.utils.helpers import ensure_directories


LOGGER = logging.getLogger("influencesphere.son")


def _apriori(
    transactions: Sequence[tuple[int, ...]],
    minimum_count: int,
    max_itemset_size: int,
) -> set[tuple[int, ...]]:
    """Return local frequent itemsets using an in-memory Apriori pass."""
    if not transactions:
        return set()
    item_counts: dict[tuple[int, ...], int] = {}
    for basket in transactions:
        for item in basket:
            item_counts[(item,)] = item_counts.get((item,), 0) + 1
    frequent = {
        itemset for itemset, count in item_counts.items() if count >= minimum_count
    }
    all_frequent = set(frequent)
    previous = frequent
    for size in range(2, max_itemset_size + 1):
        parents = sorted(previous)
        candidates = {
            tuple(sorted(left + right))
            for left, right in combinations(parents, 2)
            if len(set(left + right)) == size
            and all(
                subset in previous
                for subset in combinations(tuple(sorted(set(left + right))), size - 1)
            )
        }
        counts = {candidate: 0 for candidate in candidates}
        for basket in transactions:
            basket_set = set(basket)
            for candidate in candidates:
                if set(candidate).issubset(basket_set):
                    counts[candidate] += 1
        previous = {
            itemset for itemset, count in counts.items() if count >= minimum_count
        }
        all_frequent.update(previous)
        if not previous:
            break
    return all_frequent


def brute_force_frequent_itemsets(
    transactions: Sequence[tuple[int, ...]],
    minimum_count: int,
    max_itemset_size: int,
) -> set[tuple[int, ...]]:
    """Compute reference frequent itemsets for small validation datasets."""
    items = sorted({item for transaction in transactions for item in transaction})
    result: set[tuple[int, ...]] = set()
    for size in range(1, max_itemset_size + 1):
        for itemset in combinations(items, size):
            count = sum(
                set(itemset).issubset(transaction) for transaction in transactions
            )
            if count >= minimum_count:
                result.add(itemset)
    return result


def validate_no_false_negatives(
    transactions: Sequence[tuple[int, ...]],
    itemsets: Iterable[tuple[int, ...]],
    minimum_count: int,
    max_itemset_size: int,
) -> bool:
    """Check SON results contain every reference frequent itemset."""
    expected = brute_force_frequent_itemsets(
        transactions, minimum_count, max_itemset_size
    )
    return expected.issubset(set(itemsets))


def _partition_candidates(
    iterator: Iterator[tuple[int, ...]],
    min_support: float,
    max_itemset_size: int,
) -> Iterator[tuple[int, ...]]:
    transactions = list(iterator)
    minimum_count = max(1, math.ceil(min_support * len(transactions)))
    return iter(_apriori(transactions, minimum_count, max_itemset_size))


def _normalize_transactions(
    edges: DataFrame,
    max_items_per_basket: int | None,
) -> "pyspark.RDD[tuple[int, ...]]":
    from pyspark.sql.functions import collect_set, sort_array

    transactions = (
        edges.groupBy("src")
        .agg(sort_array(collect_set("dst")).alias("items"))
        .select("items")
        .rdd.map(lambda row: tuple(int(item) for item in row["items"]))
    )
    if max_items_per_basket is not None:
        transactions = transactions.map(
            lambda items: items[:max_items_per_basket]
        )
    return transactions.filter(bool)


def run_son(
    spark: SparkSession,
    output_dir: Path,
    *,
    config: AppConfig | None = None,
    min_support: float = 0.01,
    max_items_per_basket: int | None = 1000,
    max_itemset_size: int = 3,
    prefilter_items: bool = True,
) -> dict:
    """Run two-phase SON and persist frequent itemsets and evaluation data."""
    config = config or load_config()
    if not 0 < min_support <= 1:
        raise ValueError("min_support must be greater than 0 and at most 1")
    if max_items_per_basket is not None and max_items_per_basket <= 0:
        raise ValueError("max_items_per_basket must be positive or None")
    if max_itemset_size <= 0:
        raise ValueError("max_itemset_size must be positive")

    output_dir = Path(output_dir)
    ensure_directories(output_dir)
    started = time.perf_counter()
    edges = spark.read.parquet(
        str(config.paths.data_processed / "cleaned_directed_edges")
    ).select("src", "dst")
    transactions = _normalize_transactions(edges, max_items_per_basket).persist()
    transaction_count = transactions.count()
    if transaction_count == 0:
        raise ValueError("No outgoing-neighbor transactions were found")

    global_minimum = max(1, math.ceil(min_support * transaction_count))
    if prefilter_items:
        frequent_items = set(
            transactions.flatMap(lambda basket: set(basket))
            .map(lambda item: (item, 1))
            .reduceByKey(lambda left, right: left + right)
            .filter(lambda pair: pair[1] >= global_minimum)
            .keys()
            .collect()
        )
        broadcast_items = spark.sparkContext.broadcast(frequent_items)
        filtered_transactions = transactions.map(
            lambda basket: tuple(item for item in basket if item in broadcast_items.value)
        ).filter(bool).persist()
        filtered_count = filtered_transactions.count()
        transactions.unpersist()
        transactions = filtered_transactions
        transaction_count = filtered_count
        broadcast_items.unpersist()
        global_minimum = max(1, math.ceil(min_support * transaction_count))

    partition_count = transactions.getNumPartitions()
    local_minimum = max(1, math.ceil(min_support * transaction_count / partition_count))
    candidates = transactions.mapPartitions(
        lambda iterator: _partition_candidates(
            iterator, min_support, max_itemset_size
        )
    ).distinct()
    candidate_values = candidates.collect()
    candidate_broadcast = spark.sparkContext.broadcast(
        {tuple(candidate) for candidate in candidate_values}
    )

    frequent_counts = (
        transactions.flatMap(
            lambda basket: (
                (itemset, 1)
                for size in range(1, min(max_itemset_size, len(basket)) + 1)
                for itemset in combinations(basket, size)
                if itemset in candidate_broadcast.value
            )
        )
        .reduceByKey(lambda left, right: left + right)
        .filter(lambda pair: pair[1] >= global_minimum)
    )
    frequent = frequent_counts.collect()
    candidate_broadcast.unpersist()
    transactions.unpersist()

    rows = [
        {
            "itemset": json.dumps(list(itemset), separators=(",", ":")),
            "support_count": int(count),
            "support_percent": float(count / transaction_count * 100.0),
        }
        for itemset, count in sorted(frequent)
    ]
    result_df = spark.createDataFrame(rows) if rows else spark.createDataFrame(
        [], "itemset string, support_count long, support_percent double"
    )
    result_df.write.mode("overwrite").parquet(str(output_dir / "frequent_itemsets"))
    result_df.coalesce(1).write.mode("overwrite").option("header", True).csv(
        str(output_dir / "frequent_itemsets.csv")
    )
    evaluation = {
        "min_support": min_support,
        "global_minimum_count": global_minimum,
        "local_minimum_count": local_minimum,
        "transaction_count": transaction_count,
        "candidate_count": len(candidate_values),
        "frequent_itemset_count": len(rows),
        "max_items_per_basket": max_items_per_basket,
        "max_itemset_size": max_itemset_size,
        "prefilter_items": prefilter_items,
        "execution_seconds": time.perf_counter() - started,
    }
    (output_dir / "son_evaluation.json").write_text(
        json.dumps(evaluation, indent=2) + "\n",
        encoding="utf-8",
    )
    return evaluation


def run_son_thresholds(
    spark: SparkSession,
    output_root: Path,
    thresholds: Iterable[float],
    *,
    config: AppConfig | None = None,
    **kwargs: object,
) -> dict[str, dict]:
    """Run SON independently for several support thresholds."""
    results = {}
    for threshold in thresholds:
        name = f"son_{threshold:g}"
        results[name] = run_son(
            spark,
            Path(output_root) / name,
            config=config,
            min_support=threshold,
            **kwargs,
        )
    return results


def run_configured_son(
    output_dir: Path | None = None,
    *,
    config: AppConfig | None = None,
    thresholds: Iterable[float] = (0.01,),
    **kwargs: object,
) -> dict[str, dict]:
    """Create a Spark session and run SON for configured thresholds."""
    config = config or load_config()
    from src.spark_session import create_spark_session

    spark = create_spark_session(config)
    try:
        return run_son_thresholds(
            spark,
            output_dir or config.paths.results,
            thresholds,
            config=config,
            **kwargs,
        )
    finally:
        spark.stop()
