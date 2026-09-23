"""Small shared primitives for local, single-writer incremental releases."""
from contextlib import contextmanager

import hashlib
import json
import os
from pathlib import Path
if os.name == "nt":
    import msvcrt
else:
    import fcntl
from delta.tables import DeltaTable
from pyspark.sql import functions as F


def digest(path):
    result = hashlib.sha256()
    with Path(path).open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            result.update(block)
    return result.hexdigest()


def save_json(path, value):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(".tmp")
    temporary.write_text(json.dumps(value, indent=2, sort_keys=True, default=str) + "\n")
    temporary.replace(path)


@contextmanager
def writer_lock(root):
    """
    Cross-platform single-writer lock.

    Windows uses msvcrt.
    Linux/macOS use fcntl.
    """

    root = Path(root)
    root.mkdir(parents=True, exist_ok=True)

    lock_path = root / ".lock"

    # Binary mode works with the Windows byte-range lock.
    with lock_path.open("a+b") as stream:

        if os.name == "nt":
            # msvcrt.locking() needs at least one byte
            # available in the file to lock.
            stream.seek(0, 2)

            if stream.tell() == 0:
                stream.write(b"\0")
                stream.flush()

            stream.seek(0)

            try:
                msvcrt.locking(
                    stream.fileno(),
                    msvcrt.LK_NBLCK,
                    1,
                )

            except OSError as error:
                raise RuntimeError(
                    "Another incremental writer "
                    "is already running."
                ) from error

            try:
                yield

            finally:
                stream.seek(0)

                msvcrt.locking(
                    stream.fileno(),
                    msvcrt.LK_UNLCK,
                    1,
                )

        else:
            try:
                fcntl.flock(
                    stream,
                    fcntl.LOCK_EX
                    | fcntl.LOCK_NB,
                )

            except BlockingIOError as error:
                raise RuntimeError(
                    "Another incremental writer "
                    "is already running."
                ) from error

            try:
                yield

            finally:
                fcntl.flock(
                    stream,
                    fcntl.LOCK_UN,
                )

def read(spark, path, version=None):
    reader = spark.read.format("delta")
    if version is not None:
        reader = reader.option("versionAsOf", version)
    return reader.load(str(path))


def version(spark, path):
    return int(DeltaTable.forPath(spark, str(path)).history(1).first().version)


def align(df, schema):
    for field in schema:
        if field.name not in df.columns:
            df = df.withColumn(field.name, F.lit(None).cast(field.dataType))
    return df


def fit_legacy_raw(df, existing):
    """Bridge pre-contract inferred tables without rewriting historical files.

    Reject lossy numeric conversions. Keep the original contract values in a
    JSON payload whenever a legacy physical type needs adaptation.
    """
    target_types = {field.name: field.dataType for field in existing.schema}
    mismatches = [field for field in df.schema if field.name in target_types
                  and not field.name.startswith("_") and field.dataType != target_types[field.name]]
    if not mismatches:
        return df
    if "_source_payload" not in df.columns:
        df = df.withColumn("_source_payload", F.to_json(
            F.struct(*[c for c in df.columns if not c.startswith("_")]),
            options={"ignoreNullFields": "false"}))
    invalid = F.lit(False)
    converted = {}
    for field in mismatches:
        name = field.name
        target = target_types[name]
        value = F.col(name)
        if name in ("time_local", "time_gmt") and target.simpleString() == "timestamp":
            # The old reader invented a date on its time-only columns. That
            # date is not part of the key; cleaning extracts wall-clock time.
            clock = F.regexp_extract(value.cast("string"), r"(\d{1,2}:\d{2}(?::\d{2})?)$", 1)
            cast = F.to_timestamp(F.concat(F.lit("2000-01-01 "), clock))
            invalid = invalid | (value.isNotNull() & cast.isNull())
        else:
            cast = value.cast(target)
            if target.simpleString() in ("int", "bigint", "double", "float", "smallint", "tinyint"):
                # Numeric codes with leading zeros are semantically the same
                # key; their exact source spelling remains in the payload.
                invalid = invalid | (value.isNotNull() &
                    (cast.isNull() | ~cast.cast("double").eqNullSafe(value.cast("double"))))
            else:
                invalid = invalid | (value.isNotNull() &
                    (cast.isNull() | ~cast.cast(field.dataType).eqNullSafe(value)))
        converted[name] = cast
    if df.filter(invalid).limit(1).count():
        raise ValueError("Update cannot fit legacy raw types without loss; an explicit type migration is required")
    return df.select(*[converted.get(name, F.col(name)).alias(name) for name in df.columns])


def changed_rows(incoming, existing, keys):
    """Return inserts/changes using null-safe values, not ingestion metadata."""
    existing = align(existing, incoming.schema)
    joined = incoming.alias("s").join(existing.alias("t"), keys, "left")
    equal = F.lit(True)
    for column in incoming.columns:
        if column not in keys:
            equal = equal & F.col(f"s.`{column}`").eqNullSafe(F.col(f"t.`{column}`"))
    return joined.filter(F.col(f"t.`{keys[0]}`").isNull() | ~equal).select("s.*")


def merge_rows(df, path, keys, partitions=None, insert_only=False):
    spark = df.sparkSession
    path = str(path)
    if not DeltaTable.isDeltaTable(spark, path):
        writer = df.write.format("delta").mode("overwrite")
        if partitions:
            writer = writer.partitionBy(*partitions)
        writer.save(path)
        return
    target = DeltaTable.forPath(spark, path)
    df = align(df, target.toDF().schema)
    condition = " AND ".join(f"t.`{key}` <=> s.`{key}`" for key in keys)
    builder = target.alias("t").merge(df.alias("s"), condition).withSchemaEvolution()
    if not insert_only:
        builder = builder.whenMatchedUpdateAll()
    builder.whenNotMatchedInsertAll().execute()


def replace_scope(df, path, predicate):
    """Atomically replace complete affected groups, including groups now empty."""
    writer = df.write.format("delta").mode("overwrite").option("mergeSchema", "true")
    if DeltaTable.isDeltaTable(df.sparkSession, str(path)):
        writer = writer.option("replaceWhere", predicate)
    writer.save(str(path))


def values_predicate(column, values):
    parts = []
    for value in values:
        if value is None:
            parts.append(f"`{column}` IS NULL")
        else:
            literal = str(value).replace("'", "''")
            parts.append(f"`{column}` = '{literal}'")
    return "(" + " OR ".join(parts) + ")" if parts else "false"


def month_predicate(rows):
    return " OR ".join(
        f"(pickup_year = {int(row[0])} AND pickup_month = {int(row[1])})"
        for row in rows
    ) or "false"
