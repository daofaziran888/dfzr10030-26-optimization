"""监控分析分批执行编排服务。

改造前 ``MonitorService.run_analysis`` 在一次请求里展开全部样本，样本稍大就会长时间
等待，超时后也无法判断哪些分组已经完成。本服务把一次分析拆成可持久化、可恢复的批次：

- 提交时读取监控面板记录与成功率/平均延迟指标，冻结样本集合并生成监控指标快照；
- 样本按固定单批容量拆成多个分批（chunk，即进度上的“分组”），形成监控分析批次清单；
- 每个分批完成后结果立即落库，重新执行只处理未完成分批，已完成分组不再重复计算；
- 重复提交通过幂等键/活动批次复用规则只生效一次，历史批次保留原状态；
- 所有写操作在 store 事务内进行，落库失败整体回滚到原批次状态；
- 采集事件与分析并发时支持两种样本版本策略，且保证同一分析批次绝不混入两个样本版本。
"""
from __future__ import annotations

import random
from datetime import datetime
from typing import Any

from app.store import PersistError, store

MONITOR_MODULE = "monitor"
DEFAULT_BATCH_SIZE = 5
GROUP_COUNT = 3
STRATEGY_FREEZE = "freeze"          # 冻结当前样本集合，新事件进入下一批次
STRATEGY_NEW_BATCH = "new_batch"    # 未处理分组改用新样本集合，已完成分组保留在原批次
CONFLICT_STRATEGIES = (STRATEGY_FREEZE, STRATEGY_NEW_BATCH)

BATCH_STATUSES = ("待执行", "执行中", "已停止", "已完成")
CHUNK_STATUSES = ("待执行", "执行中", "已停止", "已完成")

BATCH_SEQ = "monitor_analysis_batch"
EVENT_SEQ = "monitor_collection_event"
SAMPLES_BLOB = "monitor:sample_registry"
BATCHES_BLOB = "monitor:batches"
BATCH_INDEX_BLOB = "monitor:batch_index"
EVENTS_BLOB = "monitor:events"


def _now() -> str:
    return datetime.now().isoformat(timespec="seconds")


def _fail_flag_of(params: dict[str, Any], fail_flag: str | None) -> str | None:
    """失败注入既可以显式传参，也可以放在请求 values 的 _fail_flag 里（HTTP 路由用）。"""
    return fail_flag or (str(params["_fail_flag"]) if params.get("_fail_flag") else None)


def _arm_fail_flag(params: dict[str, Any], fail_flag: str | None) -> str | None:
    """HTTP 请求把失败开关放在 values 里时，先在当前事务内写入 store 供提交前检测。"""
    flag = _fail_flag_of(params, fail_flag)
    if flag and params.get("_fail_flag"):
        store.blob_put(flag, True)
    return flag


def _to_float(value: Any, default: float) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


class MonitorBatchService:
    # ------------------------------------------------------------------
    # 面板与样本注册表
    # ------------------------------------------------------------------
    def resolve_panel(self, panel_code: str) -> dict[str, Any]:
        """按面板编号定位监控面板记录，兼容 MON-xxxx 与种子数据 MONI-xxxx 两种写法。"""
        code = (panel_code or "").strip()
        aliases = {code}
        if code.startswith("MON-"):
            aliases.add("MONI-" + code[4:])
        elif code.startswith("MONI-"):
            aliases.add("MON-" + code[5:])
        for row in store.rows(MONITOR_MODULE):
            if row.get("面板编号") in aliases:
                return row
        raise KeyError(f"监控面板 {panel_code} 不存在")

    def _load_registry(self, panel_code: str) -> dict[str, Any]:
        registries = store.blob_get(SAMPLES_BLOB, {})
        return registries.get(panel_code) or {
            "panel_code": panel_code,
            "version": 0,
            "samples": [],
            "updated_at": None,
        }

    def _save_registry(self, panel_code: str, registry: dict[str, Any]) -> None:
        registries = store.blob_get(SAMPLES_BLOB, {})
        registries[panel_code] = registry
        store.blob_put(SAMPLES_BLOB, registries)

    def _ensure_samples(self, registry: dict[str, Any], sample_count: int,
                        event_id: str | None = None) -> bool:
        """把注册表补齐到 sample_count 个样本；返回是否发生了样本版本变更。"""
        existing = len(registry["samples"])
        if sample_count <= existing:
            return False
        registry["version"] = int(registry.get("version", 0)) + 1
        for i in range(existing, sample_count):
            sample_id = f"SMP-{i + 1:04d}"
            rng = random.Random(f"sample|{registry['panel_code']}|{sample_id}")
            registry["samples"].append({
                "sample_id": sample_id,
                "panel_code": registry["panel_code"],
                "source": f"采集源{(i % GROUP_COUNT) + 1}",
                "version": registry["version"],
                "arrived_event_id": event_id,
                "arrived_at": _now(),
                "success_rate": round(rng.uniform(0.78, 0.99), 4),
                "avg_latency_ms": round(rng.uniform(80, 420), 2),
            })
        registry["updated_at"] = _now()
        return True

    def metric_snapshot(self, panel: dict[str, Any]) -> dict[str, Any]:
        """读取监控记录中的成功率、平均延迟等指标，生成不可变的批次指标快照。"""
        rng = random.Random(f"snapshot|{panel.get('面板编号')}")
        success_rate = _to_float(panel.get("成功率"), round(rng.uniform(0.82, 0.97), 4))
        avg_latency = _to_float(panel.get("平均延迟"), round(rng.uniform(120, 380), 2))
        return {
            "面板编号": panel.get("面板编号"),
            "采集源总数": panel.get("采集源总数"),
            "今日采集量": panel.get("今日采集量"),
            "成功率": success_rate,
            "平均延迟": avg_latency,
            "失败重试": panel.get("失败重试"),
            "异常告警": panel.get("异常告警"),
            "更新时间": panel.get("更新时间"),
            "snapshot_at": _now(),
        }

    # ------------------------------------------------------------------
    # 批次清单的拆分与读取
    # ------------------------------------------------------------------
    def _frozen_samples(self, registry: dict[str, Any], sample_version: int) -> list[dict[str, Any]]:
        """冻结本次样本集合：批次内样本统一标注同一个样本版本，保证不混入两个版本。"""
        return [
            {
                "sample_id": s["sample_id"],
                "分组": f"分组{(i % GROUP_COUNT) + 1}",
                "版本": sample_version,
                "success_rate": s["success_rate"],
                "avg_latency_ms": s["avg_latency_ms"],
            }
            for i, s in enumerate(registry["samples"])
        ]

    def _build_chunks(self, batch_no: str, frozen_samples: list[dict[str, Any]],
                      batch_size: int, sample_version: int) -> list[dict[str, Any]]:
        chunks: list[dict[str, Any]] = []
        for start in range(0, len(frozen_samples), batch_size):
            part = frozen_samples[start:start + batch_size]
            chunk_no = len(chunks) + 1
            chunks.append({
                "chunk_id": f"{batch_no}-C{chunk_no:02d}",
                "chunk_no": chunk_no,
                "分组标签": sorted({s["分组"] for s in part}),
                "样本区间": [start + 1, start + len(part)],
                "sample_ids": [s["sample_id"] for s in part],
                "sample_version": sample_version,
                "status": "待执行",
                "result": None,
                "started_at": None,
                "finished_at": None,
            })
        return chunks

    def _load_batches(self) -> dict[str, dict[str, Any]]:
        return store.blob_get(BATCHES_BLOB, {})

    def _save_batches(self, batches: dict[str, dict[str, Any]]) -> None:
        store.blob_put(BATCHES_BLOB, batches)

    def _index_add(self, panel_code: str, batch_no: str) -> None:
        index = store.blob_get(BATCH_INDEX_BLOB, {})
        index.setdefault(panel_code, []).append(batch_no)
        store.blob_put(BATCH_INDEX_BLOB, index)

    def get_batch(self, batch_no: str) -> dict[str, Any]:
        batches = self._load_batches()
        batch = batches.get(batch_no)
        if batch is None:
            raise KeyError(f"监控分析批次 {batch_no} 不存在")
        return batch

    def get_manifest(self, batch_no: str) -> dict[str, Any]:
        """对外批次清单视图：监控页/报告页按同一批次编号回查都走这里。"""
        return self._manifest(self.get_batch(batch_no))

    def list_batches(self, panel_code: str | None = None) -> list[dict[str, Any]]:
        batches = self._load_batches()
        items = list(batches.values())
        if panel_code:
            panel = self.resolve_panel(panel_code)
            items = [b for b in items if b["panel_code"] == panel["面板编号"]]
        return [self._manifest(b) for b in items]

    def _active_batch(self, panel_code: str,
                      batches: dict[str, dict[str, Any]]) -> dict[str, Any] | None:
        for batch_no in store.blob_get(BATCH_INDEX_BLOB, {}).get(panel_code, []):
            batch = batches.get(batch_no)
            if batch and batch["status"] in ("待执行", "执行中", "已停止"):
                return batch
        return None

    def _manifest(self, batch: dict[str, Any]) -> dict[str, Any]:
        """对外的监控分析批次清单视图（深拷贝，避免调用方修改落库实体）。"""
        chunks = [
            {
                "chunk_id": c["chunk_id"],
                "chunk_no": c["chunk_no"],
                "分组标签": c["分组标签"],
                "样本区间": c["样本区间"],
                "样本数": len(c["sample_ids"]),
                "sample_version": c["sample_version"],
                "status": c["status"],
                "started_at": c["started_at"],
                "finished_at": c["finished_at"],
                "result": c["result"],
                "来源批次": c.get("来源批次"),
            }
            for c in batch["chunks"]
        ]
        finished = [c for c in chunks if c["status"] == "已完成"]
        return {
            "batch_no": batch["batch_no"],
            "panel_code": batch["panel_code"],
            "status": batch["status"],
            "conflict_policy": batch["conflict_policy"],
            "batch_size": batch["batch_size"],
            "sample_count": batch["sample_count"],
            "sample_version": batch["sample_version"],
            "idempotency_key": batch["idempotency_key"],
            "submitted_at": batch["submitted_at"],
            "updated_at": batch["updated_at"],
            "finished_at": batch.get("finished_at"),
            "metric_snapshot": batch["metric_snapshot"],
            "progress": {
                "总分组数": len(chunks),
                "已完成分组": len(finished),
                "待处理分组": sum(1 for c in chunks if c["status"] == "待执行"),
                "已停止分组": sum(1 for c in chunks if c["status"] == "已停止"),
            },
            "chunks": chunks,
            "carried_chunks": batch.get("carried_chunks", []),
            "predecessor_batch_no": batch.get("predecessor_batch_no"),
            "successor_batch_no": batch.get("successor_batch_no"),
            "pending_event_ids": batch.get("pending_event_ids", []),
        }

    # ------------------------------------------------------------------
    # 提交分析：幂等建批次，冻结样本，生成分批清单与指标快照
    # ------------------------------------------------------------------
    def submit_analysis(self, params: dict[str, Any], fail_flag: str | None = None) -> dict[str, Any]:
        panel_code_raw = str(params.get("面板编号") or params.get("panel") or "").strip()
        panel = self.resolve_panel(panel_code_raw)
        panel_code = panel["面板编号"]
        sample_count = max(1, int(params.get("样本范围") or params.get("sample_count") or 6))
        batch_size = max(1, int(params.get("单批容量") or params.get("batch_size")
                                 or DEFAULT_BATCH_SIZE))
        policy = params.get("冲突策略") or STRATEGY_FREEZE
        if policy not in CONFLICT_STRATEGIES:
            raise ValueError(f"未知样本冲突策略：{policy}")
        idem_key = (params.get("idempotency_key") or params.get("幂等键")
                    or f"{panel_code}:{sample_count}:{batch_size}:{policy}")

        with store.transaction(_arm_fail_flag(params, fail_flag)):
            registry = self._load_registry(panel_code)
            self._ensure_samples(registry, sample_count)
            self._save_registry(panel_code, registry)

            batches = self._load_batches()
            # 幂等规则 1：显式幂等键命中，直接复用同一批次结果
            for batch in batches.values():
                if batch.get("idempotency_key") == idem_key and batch["panel_code"] == panel_code:
                    return {"reused": True, "reason": "幂等键命中，重复提交只生效一次",
                            "batch": self._manifest(batch)}
            # 幂等规则 2：同面板存在未终结批次，重复请求复用同一批次
            active = self._active_batch(panel_code, batches)
            if active is not None:
                return {"reused": True, "reason": "存在未完成批次，复用同一批次结果",
                        "batch": self._manifest(active)}

            sample_version = registry["version"]
            seq = store.next_seq(BATCH_SEQ)
            batch_no = f"MAB-{seq:06d}"
            frozen = self._frozen_samples(registry, sample_version)
            batch = {
                "batch_no": batch_no,
                "panel_code": panel_code,
                "idempotency_key": idem_key,
                "conflict_policy": policy,
                "status": "待执行",
                "batch_size": batch_size,
                "sample_count": len(frozen),
                "sample_version": sample_version,
                "submitted_at": _now(),
                "updated_at": _now(),
                "finished_at": None,
                "metric_snapshot": self.metric_snapshot(panel),
                "samples": frozen,
                "chunks": self._build_chunks(batch_no, frozen, batch_size, sample_version),
                "carried_chunks": [],
                "predecessor_batch_no": None,
                "successor_batch_no": None,
                "pending_event_ids": [],
            }
            batches[batch_no] = batch
            self._save_batches(batches)
            self._index_add(panel_code, batch_no)
            return {"reused": False, "reason": "已创建监控分析批次",
                    "batch": self._manifest(batch)}

    # ------------------------------------------------------------------
    # 执行/继续执行：只处理未完成分组，已完成分组不重算
    # ------------------------------------------------------------------
    def _run_chunk(self, batch: dict[str, Any], chunk: dict[str, Any]) -> dict[str, Any]:
        """确定性计算分组结果：同批次同分组重复进入永远得到相同数值。"""
        rng = random.Random(
            f"group-result|{batch['batch_no']}|{chunk['chunk_no']}|v{chunk['sample_version']}"
        )
        sample_lookup = {s["sample_id"]: s for s in batch["samples"]}
        items: list[dict[str, Any]] = []
        for sample_id in chunk["sample_ids"]:
            sample = sample_lookup[sample_id]
            value = round(0.5 * sample["success_rate"] + rng.uniform(0.12, 0.45), 4)
            baseline = round(0.5 * sample["success_rate"] + rng.uniform(0.08, 0.4), 4)
            items.append({
                "id": len(items) + 1,
                "sample_id": sample_id,
                "分组": sample["分组"],
                "指标值": value,
                "对比基准": baseline,
                "差异": round(value - baseline, 4),
                "成功率": sample["success_rate"],
                "平均延迟": sample["avg_latency_ms"],
            })
        return {
            "items": items,
            "aggregated": {
                "分组": "/".join(chunk["分组标签"]),
                "样本数": len(items),
                "指标值": round(sum(i["指标值"] for i in items) / len(items), 4),
                "成功率": round(sum(i["成功率"] for i in items) / len(items), 4),
                "平均延迟": round(sum(i["平均延迟"] for i in items) / len(items), 2),
            },
        }

    def _refresh_status(self, batch: dict[str, Any]) -> None:
        statuses = [c["status"] for c in batch["chunks"]]
        if any(s in ("待执行", "执行中") for s in statuses):
            batch["status"] = "执行中" if any(s == "已完成" for s in statuses) else "待执行"
        elif any(s == "已停止" for s in statuses):
            batch["status"] = "已停止"
            batch["finished_at"] = batch.get("finished_at") or _now()
        else:
            batch["status"] = "已完成"
            batch["finished_at"] = _now()
        batch["updated_at"] = _now()

    def execute_batch(self, batch_no: str, limit: int | None = None,
                      fail_flag: str | None = None) -> dict[str, Any]:
        with store.transaction(fail_flag):
            batches = self._load_batches()
            batch = batches.get(batch_no)
            if batch is None:
                raise KeyError(f"监控分析批次 {batch_no} 不存在")
            if batch["status"] == "已完成":
                return {"processed": 0, "reason": "批次已完成，结果直接复用",
                        "batch": self._manifest(batch)}

            # 批次停止后继续执行：恢复未完成分组，已完成分组保持原样不重算
            resumed = batch["status"] == "已停止"
            processed = 0
            for chunk in batch["chunks"]:
                if chunk["status"] == "已完成":
                    continue
                if limit is not None and processed >= limit:
                    break
                if chunk["status"] == "已停止":
                    chunk["status"] = "待执行"
                chunk["status"] = "执行中"
                chunk["started_at"] = chunk["started_at"] or _now()
                chunk["result"] = self._run_chunk(batch, chunk)
                chunk["status"] = "已完成"
                chunk["finished_at"] = _now()
                processed += 1
            self._refresh_status(batch)
            self._save_batches(batches)
            reason = "批次曾停止，仅继续处理未完成分组" if resumed and processed else "没有待处理分组"
            return {"processed": processed, "reason": reason,
                    "batch": self._manifest(batch)}

    def stop_batch(self, batch_no: str, fail_flag: str | None = None) -> dict[str, Any]:
        with store.transaction(fail_flag):
            batches = self._load_batches()
            batch = batches.get(batch_no)
            if batch is None:
                raise KeyError(f"监控分析批次 {batch_no} 不存在")
            if batch["status"] in ("已完成", "已停止"):
                return {"changed": False, "reason": f"批次处于{batch['status']}，停止操作幂等返回",
                        "batch": self._manifest(batch)}
            for chunk in batch["chunks"]:
                if chunk["status"] in ("待执行", "执行中"):
                    chunk["status"] = "已停止"
                    chunk["finished_at"] = _now()
            batch["status"] = "已停止"
            batch["finished_at"] = _now()
            batch["updated_at"] = _now()
            self._save_batches(batches)
            return {"changed": True, "reason": "批次已停止，已完成分组保留，其余分组待继续执行",
                    "batch": self._manifest(batch)}

    # ------------------------------------------------------------------
    # 采集事件并发：两种样本版本策略，绝不混版本
    # ------------------------------------------------------------------
    def record_event(self, params: dict[str, Any], fail_flag: str | None = None) -> dict[str, Any]:
        panel = self.resolve_panel(str(params.get("面板编号") or params.get("panel") or ""))
        panel_code = panel["面板编号"]
        new_count = max(1, int(params.get("事件数量") or params.get("sample_count") or 1))
        source = params.get("来源标识") or "实时采集事件"

        with store.transaction(_arm_fail_flag(params, fail_flag)):
            events = store.blob_get(EVENTS_BLOB, [])
            event_id = f"EVT-{store.next_seq(EVENT_SEQ):06d}"
            events.append({
                "event_id": event_id,
                "panel_code": panel_code,
                "来源标识": source,
                "新增样本数": new_count,
                "创建时间": _now(),
                "处理方式": None,
                "影响批次": [],
            })

            registry = self._load_registry(panel_code)
            target_total = len(registry["samples"]) + new_count
            self._ensure_samples(registry, target_total, event_id=event_id)
            self._save_registry(panel_code, registry)

            batches = self._load_batches()
            active = self._active_batch(panel_code, batches)
            if active is None:
                events[-1]["处理方式"] = "无活动批次，新事件进入下一批次"
                store.blob_put(EVENTS_BLOB, events)
                return {"event_id": event_id, "处理方式": events[-1]["处理方式"],
                        "affected_batches": []}

            if active["conflict_policy"] == STRATEGY_FREEZE:
                # 策略一：冻结当前样本集合，当前批次维持原版本，新事件只登记到下一批次
                active.setdefault("pending_event_ids", []).append(event_id)
                active["updated_at"] = _now()
                events[-1]["处理方式"] = "冻结当前样本集合，新事件进入下一批次"
                events[-1]["影响批次"] = [active["batch_no"]]
                self._save_batches(batches)
                store.blob_put(EVENTS_BLOB, events)
                return {"event_id": event_id, "处理方式": events[-1]["处理方式"],
                        "affected_batches": [active["batch_no"]],
                        "batch": self._manifest(active)}

            # 策略二：原批次保留已完成分组并停止，未处理分组转到使用新样本集合的后继批次
            carried = [
                {
                    "batch_no": active["batch_no"],
                    "chunk_id": c["chunk_id"],
                    "分组标签": c["分组标签"],
                    "sample_version": c["sample_version"],
                    "result_summary": c["result"]["aggregated"] if c["result"] else None,
                }
                for c in active["chunks"] if c["status"] == "已完成"
            ]
            for c in active["chunks"]:
                if c["status"] != "已完成":
                    c["status"] = "已停止"
                    c["finished_at"] = _now()
            active["status"] = "已停止"
            active["finished_at"] = _now()
            active["updated_at"] = _now()

            sample_version = registry["version"]
            seq = store.next_seq(BATCH_SEQ)
            successor_no = f"MAB-{seq:06d}"
            frozen = self._frozen_samples(registry, sample_version)
            successor = {
                "batch_no": successor_no,
                "panel_code": panel_code,
                "idempotency_key": f"event-successor:{event_id}:{successor_no}",
                "conflict_policy": STRATEGY_NEW_BATCH,
                "status": "待执行",
                "batch_size": active["batch_size"],
                "sample_count": len(frozen),
                "sample_version": sample_version,
                "submitted_at": _now(),
                "updated_at": _now(),
                "finished_at": None,
                "metric_snapshot": self.metric_snapshot(panel),
                "samples": frozen,
                "chunks": self._build_chunks(successor_no, frozen,
                                             active["batch_size"], sample_version),
                "carried_chunks": carried,
                "predecessor_batch_no": active["batch_no"],
                "successor_batch_no": None,
                "pending_event_ids": [event_id],
            }
            active["successor_batch_no"] = successor_no
            batches[successor_no] = successor
            self._save_batches(batches)
            self._index_add(panel_code, successor_no)

            events[-1]["处理方式"] = "未处理分组改用新样本集合，已完成分组保留在原批次"
            events[-1]["影响批次"] = [active["batch_no"], successor_no]
            store.blob_put(EVENTS_BLOB, events)
            return {"event_id": event_id, "处理方式": events[-1]["处理方式"],
                    "affected_batches": [active["batch_no"], successor_no],
                    "batch": self._manifest(successor),
                    "predecessor": self._manifest(active)}

    def list_events(self, panel_code: str | None = None) -> list[dict[str, Any]]:
        events = store.blob_get(EVENTS_BLOB, [])
        if panel_code:
            panel = self.resolve_panel(panel_code)
            events = [e for e in events if e["panel_code"] == panel["面板编号"]]
        return [dict(e) for e in events]


batch_service = MonitorBatchService()
