"""Export runtime SQL metrics, including nodes hidden behind AQE stages."""
from __future__ import annotations


def scala_map(mapping, convert):
    result = {}
    iterator = mapping.iterator()
    while iterator.hasNext():
        item = iterator.next()
        result[str(item._1())] = convert(item._2())
    return result


def executed_metrics(dataframe):
    nodes, seen = [], set()

    def visit(plan):
        kind = plan.getClass().getSimpleName()
        identity = (kind, plan.id())
        if identity in seen:
            return
        seen.add(identity)
        values = scala_map(plan.metrics(), lambda m: {
            "name": str(m.name().get()) if m.name().isDefined() else None,
            "value": m.value(), "type": str(m.metricType())})
        record = {"id": plan.id(), "node": str(plan.nodeName()),
                  "class": kind, "metrics": values}
        if kind == "FileSourceScanExec":
            record["scan_metadata"] = scala_map(plan.metadata(), str)
        nodes.append(record)
        if kind == "AdaptiveSparkPlanExec":
            visit(plan.executedPlan())
        elif kind.endswith("QueryStageExec"):
            visit(plan.plan())
        else:
            iterator = plan.children().iterator()
            while iterator.hasNext():
                visit(iterator.next())

    visit(dataframe._jdf.queryExecution().executedPlan())
    return {"nodes": nodes,
            "note": "Raw Spark SQL metric types/units; file sizes are selected full-file bytes, not physical column I/O. Shared nodes are deduplicated by plan identity."}


def scan_summary(payload):
    return [{"location": n.get("scan_metadata", {}).get("Location"),
             "partition_filters": n.get("scan_metadata", {}).get("PartitionFilters"),
             "files": n["metrics"].get("numFiles", {}).get("value"),
             "file_bytes": n["metrics"].get("filesSize", {}).get("value"),
             "output_rows": n["metrics"].get("numOutputRows", {}).get("value")}
            for n in payload["nodes"] if n["class"] == "FileSourceScanExec"]
