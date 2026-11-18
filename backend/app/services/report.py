"""采集报告 分析看板服务。

跨模块数据流：采集监控（批次编排）→ 采集报告（复核汇总）。
本服务通过 :class:`~app.services.monitor_batch.MonitorBatchService` 读取监控分析批次清单，
按批次汇总成功率与平均延迟，生成“待复核”结果；汇总只统计已完成分组，未完成/已停止
分组不计入，且对同一批次的重复请求复用同一份复核记录（幂等，不重复生成）。
"""
from __future__ import annotations

import random
from datetime import datetime
from typing import Any

from app.store import store
from app.services.monitor_batch import batch_service

MODULE = "report"
REQUIRED_FIELDS = ["报告编号", "采集源", "统计周期"]
STATUS_ORDER = ["生成中", "已生成", "已发送"]
ACTION_RULES = {"重新生成": "已生成"}
NEGATIVE_ACTIONS = []

REVIEWS_BLOB = "report:batch_reviews"
REVIEW_SEQ = "report_batch_review"


class ReportService:
    def list_results(self) -> list[dict[str, Any]]:
        rows = store.rows(MODULE)
        results: list[dict] = []
        for row in rows:
            d = dict(row)
            d["指标值"] = round(random.uniform(0.65, 0.98), 3)
            d["对比基准"] = round(random.uniform(0.60, 0.92), 3)
            d["差异"] = round(d["指标值"] - d["对比基准"], 3)
            results.append(d)
        return results

    def run_analysis(self, params: dict[str, Any]) -> list[dict[str, Any]]:
        count = int(params.get("样本范围", params.get("并行数量", "6")))
        results: list[dict] = []
        for i in range(count):
            results.append({
                "id": i + 1,
                "分组": f"分组{(i % 3) + 1}",
                "指标值": round(random.uniform(0.62, 1.02), 3),
                "对比基准": round(random.uniform(0.58, 0.95), 3),
                "差异": round(random.uniform(-0.2, 0.3), 3),
            })
        return results

    # ------------------------------------------------------------------
    # 监控批次复核：读取监控分析批次清单并按批次汇总
    # ------------------------------------------------------------------
    def _load_reviews(self) -> dict[str, dict[str, Any]]:
        return store.blob_get(REVIEWS_BLOB, {})

    def _save_reviews(self, reviews: dict[str, dict[str, Any]]) -> None:
        store.blob_put(REVIEWS_BLOB, reviews)

    def list_batch_reviews(self, panel_code: str | None = None) -> list[dict[str, Any]]:
        reviews = self._load_reviews()
        items = list(reviews.values())
        if panel_code:
            panel = batch_service.resolve_panel(panel_code)
            items = [r for r in items if r.get("面板编号") == panel["面板编号"]]
        return [dict(r) for r in items]

    def get_batch_review(self, batch_no: str) -> dict[str, Any] | None:
        return self._load_reviews().get(batch_no)

    def summarize_batch(self, params: dict[str, Any],
                        fail_flag: str | None = None) -> dict[str, Any]:
        """按监控分析批次汇总成功率与平均延迟，生成待复核结果（幂等）。"""
        batch_no = str(params.get("batch_no") or params.get("批次编号") or "").strip()
        if not batch_no:
            raise ValueError("缺少批次编号 batch_no")
        force = bool(params.get("force"))

        flag = fail_flag or (str(params["_fail_flag"]) if params.get("_fail_flag") else None)
        if flag and params.get("_fail_flag"):
            store.blob_put(flag, True)
        with store.transaction(flag):
            batch = batch_service.get_batch(batch_no)
            manifest = batch_service._manifest(batch)
            reviews = self._load_reviews()
            existing = reviews.get(batch_no)
            # 幂等：同一批次重复请求复用同一份复核结果，仅在 force 时重算已完成部分
            if existing is not None and not force:
                return {"reused": True, "reason": "同批次复核结果已存在，直接复用",
                        "review": dict(existing)}

            finished = [c for c in batch["chunks"]
                        if c["status"] == "已完成" and c["result"]]
            if not finished:
                raise ValueError(f"批次 {batch_no} 暂无已完成分组，无法汇总")

            # 仅汇总已完成分组；样本级数据保证成功率/平均延迟按样本量加权
            rows: list[dict[str, Any]] = []
            sample_total = 0
            weight_success = 0.0
            weight_latency = 0.0
            metric_total = 0.0
            for chunk in finished:
                agg = chunk["result"]["aggregated"]
                sample_count = len(chunk["sample_ids"])
                sample_total += sample_count
                weight_success += agg["成功率"] * sample_count
                weight_latency += agg["平均延迟"] * sample_count
                metric_total += agg["指标值"] * sample_count
                rows.append({
                    "chunk_id": chunk["chunk_id"],
                    "分组": agg["分组"],
                    "样本数": sample_count,
                    "成功率": agg["成功率"],
                    "平均延迟": agg["平均延迟"],
                    "指标值": agg["指标值"],
                    "来源批次": chunk.get("来源批次", batch_no),
                })

            # 后继批次会携带前序批次保留的已完成分组，一并纳入汇总但标记来源
            carried = manifest.get("carried_chunks", [])
            for carried_chunk in carried:
                summary = carried_chunk.get("result_summary") or {}
                if not summary:
                    continue
                sample_count = int(summary.get("样本数", 0))
                if not sample_count:
                    continue
                sample_total += sample_count
                weight_success += float(summary.get("成功率", 0)) * sample_count
                weight_latency += float(summary.get("平均延迟", 0)) * sample_count
                metric_total += float(summary.get("指标值", 0)) * sample_count
                rows.append({
                    "chunk_id": carried_chunk["chunk_id"],
                    "分组": summary.get("分组", ""),
                    "样本数": sample_count,
                    "成功率": summary.get("成功率"),
                    "平均延迟": summary.get("平均延迟"),
                    "指标值": summary.get("指标值"),
                    "来源批次": carried_chunk["batch_no"],
                })

            seq = store.next_seq(REVIEW_SEQ) if existing is None else int(
                existing["复核编号"].split("-")[-1])
            review = {
                "复核编号": f"RVB-{seq:06d}",
                "批次编号": batch_no,
                "面板编号": manifest["panel_code"],
                "样本版本": manifest["sample_version"],
                "批次状态": manifest["status"],
                "复核状态": "待复核",
                "已完成分组": len(finished),
                "已停止分组": manifest["progress"]["已停止分组"],
                "总分组数": manifest["progress"]["总分组数"],
                "总样本数": sample_total,
                "成功率": round(weight_success / sample_total, 4),
                "平均延迟": round(weight_latency / sample_total, 2),
                "平均指标值": round(metric_total / sample_total, 4),
                "明细": rows,
                "指标快照": manifest["metric_snapshot"],
                "生成时间": existing["生成时间"] if existing else _now(),
                "更新时间": _now(),
            }
            reviews[batch_no] = review
            self._save_reviews(reviews)
            return {"reused": False, "reason": "已按批次生成待复核结果",
                    "review": dict(review)}

    def get_entry(self, entry_id: int) -> dict[str, Any] | None:
        return store.find(MODULE, entry_id)


def _now() -> str:
    return datetime.now().isoformat(timespec="seconds")
