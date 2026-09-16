"""Order-independent validation of small analytical query results.

Hashes are exact and include duplicate rows. Floating-point tolerance is
checked separately: equal results need not have equal exact hashes.
"""
from __future__ import annotations

from collections import Counter, defaultdict
from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal
import hashlib
import json
import math

from pyspark.sql.types import DoubleType, FloatType, StructType


def canonical_value(value):
    if value is None:
        return ["null"]
    if isinstance(value, bool):
        return ["bool", value]
    if isinstance(value, int):
        return ["int", str(value)]
    if isinstance(value, float):
        return ["float", "nan" if math.isnan(value) else value.hex()]
    if isinstance(value, Decimal):
        return ["decimal", str(value)]
    if isinstance(value, datetime):
        return ["datetime", value.isoformat()]
    if isinstance(value, date):
        return ["date", value.isoformat()]
    if isinstance(value, str):
        return ["str", value]
    if isinstance(value, bytes):
        return ["bytes", value.hex()]
    if isinstance(value, dict):
        items = [(canonical_value(k), canonical_value(v)) for k, v in value.items()]
        return ["map", sorted(items, key=lambda item: json.dumps(item[0]))]
    if isinstance(value, (list, tuple)):
        return ["sequence", [canonical_value(v) for v in value]]
    raise TypeError(f"Unsupported result value: {type(value).__name__}")


def row_token(row):
    return json.dumps(canonical_value(tuple(row)), ensure_ascii=False, separators=(",", ":"))


@dataclass
class ResultSnapshot:
    schema: StructType
    rows: list[tuple]
    content_hash: str

    @classmethod
    def build(cls, schema, rows, max_rows=100_000):
        values = [tuple(row) for row in rows]
        if len(values) > max_rows:
            raise ValueError("Analytical result exceeds max_result_rows; use a distributed validator")
        # Ignore nullability/metadata, which physical rewrites may alter, but
        # require identical ordered names and Spark SQL data types.
        signature = [(f.name, f.dataType.json()) for f in schema.fields]
        payload = json.dumps([signature, sorted(row_token(row) for row in values)])
        return cls(schema, values, hashlib.sha256(payload.encode()).hexdigest())

    def artifact(self):
        return {"schema": self.schema.jsonValue(), "row_count": len(self.rows),
                "content_hash": self.content_hash,
                "canonical_rows": sorted(row_token(row) for row in self.rows)}


def _float_equal(left, right, atol, rtol):
    if left is None or right is None:
        return left is right
    if math.isnan(left) or math.isnan(right):
        return math.isnan(left) and math.isnan(right)
    return math.isclose(left, right, abs_tol=atol, rel_tol=rtol)


def _has_perfect_matching(left, right, compatible):
    """Bipartite matching preserves duplicates without greedy false failures."""
    edges = [[j for j, row in enumerate(right) if compatible(item, row)] for item in left]
    matched_right = {}
    for start in range(len(left)):
        queue, seen_left, parents = [start], {start}, {}
        endpoint = None
        for node in queue:
            for candidate in edges[node]:
                if candidate in parents:
                    continue
                parents[candidate] = node
                if candidate not in matched_right:
                    endpoint = candidate
                    break
                other = matched_right[candidate]
                if other not in seen_left:
                    seen_left.add(other)
                    queue.append(other)
            if endpoint is not None:
                break
        if endpoint is None:
            return False
        while endpoint is not None:
            node = parents[endpoint]
            previous = next((r for r, l in matched_right.items() if l == node), None)
            matched_right[endpoint] = node
            endpoint = previous
    return True


def compare_results(expected, actual, *, atol=1e-8, rtol=1e-6):
    if not math.isfinite(atol) or not math.isfinite(rtol) or atol < 0 or rtol < 0:
        raise ValueError("Tolerances must be finite and non-negative")
    signature = lambda s: [(f.name, f.dataType.json()) for f in s.fields]
    schema_equal = signature(expected.schema) == signature(actual.schema)
    count_equal = len(expected.rows) == len(actual.rows)
    exact = schema_equal and expected.content_hash == actual.content_hash
    equal = exact
    if schema_equal and count_equal and not exact:
        floats = [i for i, f in enumerate(expected.schema.fields)
                  if isinstance(f.dataType, (FloatType, DoubleType))]
        keys = [i for i in range(len(expected.schema.fields)) if i not in floats]
        groups = []
        for rows in (expected.rows, actual.rows):
            grouped = defaultdict(list)
            for row in rows:
                grouped[row_token(tuple(row[i] for i in keys))].append(tuple(row[i] for i in floats))
            groups.append(grouped)
        equal = groups[0].keys() == groups[1].keys()
        for key in groups[0] if equal else ():
            left, right = groups[0][key], groups[1][key]
            if len(left) != len(right):
                equal = False
                break
            # Remove exact matches before the tolerance matching fallback.
            lc, rc = Counter(row_token(r) for r in left), Counter(row_token(r) for r in right)
            common = lc & rc
            remaining = []
            for rows, counts in ((left, lc - common), (right, rc - common)):
                kept = []
                for row in rows:
                    token = row_token(row)
                    if counts[token]:
                        kept.append(row)
                        counts[token] -= 1
                remaining.append(kept)
            compatible = lambda a, b: all(_float_equal(x, y, atol, rtol) for x, y in zip(a, b))
            if not _has_perfect_matching(*remaining, compatible):
                equal = False
                break
    return {"schema_equal": schema_equal, "row_count_equal": count_equal,
            "exact_hash_equal": exact, "results_equal": equal,
            "expected_rows": len(expected.rows), "actual_rows": len(actual.rows),
            "expected_hash": expected.content_hash, "actual_hash": actual.content_hash,
            "absolute_tolerance": atol, "relative_tolerance": rtol}
