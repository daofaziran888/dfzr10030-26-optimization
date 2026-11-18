"""采集监控 分析看板服务：汇总指标、运行分析并生成模拟结果。

监控分析已改为分批执行，具体编排见 :mod:`app.services.monitor_batch`。
本服务保留看板列表/详情与旧版 ``run_analysis`` 入口，旧入口内部也走批次引擎，
保证历史调用方拿到的结果与批次清单中的已完成分组完全一致。
"""
from __future__ import annotations

import random
from typing import Any

from app.store import store
from app.services.monitor_batch import batch_service

MODULE = "monitor"
REQUIRED_FIELDS = ["面板编号", "采集源总数", "今日采集量"]
STATUS_ORDER = ["正常", "预警", "告警"]
ACTION_RULES = {"查看详情": "正常"}
NEGATIVE_ACTIONS = []


class MonitorService:
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
        """旧版一次出结果的入口：内部提交批次并一次性跑完所有分批。

        分批执行期间每完成一个分组就落库；即便此处超时，批次清单里也能看到
        已完成分组，重新请求会复用同一批次而不是重新计算。
        """
        submit_params = dict(params)
        submit_params.setdefault("面板编号", "MONI-0003")
        submit_params.setdefault("样本范围", params.get("样本范围", params.get("并行数量", "6")))
        submit_params.setdefault("单批容量", params.get("单批容量", 5))
        fail_flag = params.get("_fail_flag")
        submitted = batch_service.submit_analysis(submit_params, fail_flag=fail_flag)
        batch_no = submitted["batch"]["batch_no"]
        batch_service.execute_batch(batch_no, fail_flag=fail_flag)
        manifest = batch_service.get_manifest(batch_no)
        items: list[dict[str, Any]] = []
        for chunk in manifest["chunks"]:
            if chunk["status"] != "已完成" or not chunk["result"]:
                continue
            for item in chunk["result"]["items"]:
                items.append(item)
        return items

    def get_entry(self, entry_id: int) -> dict[str, Any] | None:
        return store.find(MODULE, entry_id)
