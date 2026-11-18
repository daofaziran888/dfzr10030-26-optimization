"""采集监控 分析看板接口：汇总指标、分批执行、批次清单与采集事件入口。"""
from __future__ import annotations

import csv
import io
import random
from typing import Any

from fastapi import APIRouter, HTTPException, Query, Request
from fastapi.responses import StreamingResponse

from app.schemas import ActionPayload, ActionResult, EntryPayload
from app.services.monitor import MonitorService
from app.services.monitor_batch import (
    STRATEGY_FREEZE,
    batch_service,
)
from app.store import PersistError

router = APIRouter(prefix="/api/monitor", tags=["采集监控"])
service = MonitorService()
LIST_FIELDS = ["面板编号", "采集源总数", "今日采集量", "成功率", "平均延迟", "失败重试", "异常告警", "更新时间"]
STATUSES = ["正常", "预警", "告警"]


def csv_cell(value: Any) -> str:
    text = str(value) if value is not None else ""
    if text and text[0] in "=+-@":
        text = "'" + text
    return '"' + text.replace('"', '""') + '"'


def _fail_flag(values: dict[str, Any]) -> str | None:
    """测试用失败注入：values 中携带 _fail_flag 时，对应事务模拟一次落库失败。"""
    flag = values.get("_fail_flag")
    return str(flag) if flag else None


@router.get("/summary")
def get_summary() -> dict[str, Any]:
    return {"metrics": [
        {"label": "覆盖数据源", "value": random.randint(12, 45), "trend": random.randint(-12, 25)},
        {"label": "最新指标值", "value": round(random.uniform(0.72, 0.99), 3), "trend": random.randint(-8, 15)},
        {"label": "异常记录", "value": random.randint(0, 8), "trend": -random.randint(1, 10)},
    ]}


@router.get("")
def list_results() -> dict[str, Any]:
    return {"items": service.list_results()}


@router.post("/run")
def run_analysis(payload: EntryPayload) -> dict[str, Any]:
    try:
        results = service.run_analysis(payload.values)
    except PersistError as exc:
        raise HTTPException(status_code=500, detail=f"批次事务已回滚：{exc}") from exc
    return {"items": results}


# ----------------------------------------------------------------------
# 监控分析分批执行
# ----------------------------------------------------------------------
@router.post("/batches")
def submit_batch(payload: EntryPayload) -> dict[str, Any]:
    """提交分析：读取监控记录、冻结样本集合、按固定单批容量拆分并生成批次清单。"""
    try:
        outcome = batch_service.submit_analysis(payload.values, fail_flag=_fail_flag(payload.values))
    except KeyError as exc:
        raise HTTPException(status_code=404, detail=str(exc).strip("'")) from exc
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except PersistError as exc:
        raise HTTPException(status_code=500, detail=f"提交失败，事务已回滚到原批次：{exc}") from exc
    return {"ok": True, **outcome}


@router.get("/batches")
def list_batches(panel: str | None = Query(default=None)) -> dict[str, Any]:
    try:
        items = batch_service.list_batches(panel)
    except KeyError as exc:
        raise HTTPException(status_code=404, detail=str(exc).strip("'")) from exc
    return {"items": items, "total": len(items)}


@router.get("/batches/{batch_no}")
def get_batch(batch_no: str) -> dict[str, Any]:
    """按批次编号回查清单：监控页与报告页在同一编号下重新进入看到相同结果。"""
    try:
        return batch_service.get_manifest(batch_no)
    except KeyError as exc:
        raise HTTPException(status_code=404, detail=str(exc).strip("'")) from exc


@router.post("/batches/{batch_no}/execute")
def execute_batch(batch_no: str, payload: EntryPayload | None = None) -> dict[str, Any]:
    """执行/继续执行：只处理未完成分组，已完成分组不再重复计算。"""
    values = payload.values if payload else {}
    limit = values.get("limit")
    try:
        outcome = batch_service.execute_batch(
            batch_no, limit=int(limit) if limit is not None else None,
            fail_flag=_fail_flag(values),
        )
    except KeyError as exc:
        raise HTTPException(status_code=404, detail=str(exc).strip("'")) from exc
    except PersistError as exc:
        raise HTTPException(status_code=500, detail=f"执行落库失败，已退回原批次：{exc}") from exc
    return {"ok": True, **outcome}


@router.post("/batches/{batch_no}/stop")
def stop_batch(batch_no: str, payload: EntryPayload | None = None) -> dict[str, Any]:
    """停止批次：已完成分组保留，剩余分组标记停止，等待后续继续执行。"""
    values = payload.values if payload else {}
    try:
        outcome = batch_service.stop_batch(batch_no, fail_flag=_fail_flag(values))
    except KeyError as exc:
        raise HTTPException(status_code=404, detail=str(exc).strip("'")) from exc
    except PersistError as exc:
        raise HTTPException(status_code=500, detail=f"停止落库失败，已退回原批次：{exc}") from exc
    return {"ok": True, **outcome}


@router.post("/events")
def record_event(payload: EntryPayload) -> dict[str, Any]:
    """新增采集事件：与同面板分析请求并发时按批次冲突策略处理样本版本。"""
    try:
        outcome = batch_service.record_event(payload.values, fail_flag=_fail_flag(payload.values))
    except KeyError as exc:
        raise HTTPException(status_code=404, detail=str(exc).strip("'")) from exc
    except PersistError as exc:
        raise HTTPException(status_code=500, detail=f"事件落库失败，事务已回滚：{exc}") from exc
    return {"ok": True, **outcome}


@router.get("/events")
def list_events(panel: str | None = Query(default=None)) -> dict[str, Any]:
    try:
        items = batch_service.list_events(panel)
    except KeyError as exc:
        raise HTTPException(status_code=404, detail=str(exc).strip("'")) from exc
    return {"items": items, "total": len(items)}


@router.get("/conflict-policies")
def conflict_policies() -> dict[str, Any]:
    return {"items": [
        {"key": STRATEGY_FREEZE, "label": "冻结当前样本集合，新事件进入下一批次"},
        {"key": "new_batch", "label": "未处理分组改用新样本集合，已完成分组保留"},
    ], "default": STRATEGY_FREEZE}


@router.get("/export")
def export_entries() -> StreamingResponse:
    items = service.list_results()
    buf = io.StringIO(newline="")
    writer = csv.writer(buf)
    all_keys = ["id"]
    if items:
        all_keys = list(items[0].keys())
    writer.writerow(all_keys)
    for item in items:
        writer.writerow([csv_cell(item.get(k, "")) for k in all_keys])
    buf.seek(0)
    return StreamingResponse(iter([buf.getvalue()]), media_type="text/csv; charset=utf-8",
                             headers={"Content-Disposition": "attachment; filename=monitor.csv"})


@router.get("/{entry_id}")
def get_entry(entry_id: int) -> dict:
    entry = service.get_entry(entry_id)
    if entry is None:
        raise HTTPException(status_code=404, detail=f"监控面板 {entry_id} 不存在")
    return entry
