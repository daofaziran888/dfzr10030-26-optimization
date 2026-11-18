"""内存数据仓库：给每个业务模块准备一份可筛选、可流转的示例数据。

真实项目里这里会换成数据库访问层；当前实现只依赖标准库，保证克隆下来就能起。

监控分析批次化改造后，仓库额外提供三类能力：
- ``blob_*``：监控分析批次、样本注册表等非 CRUD 实体的读写；
- ``snapshot``/``restore``：服务层事务落库失败时回滚到请求前状态；
- ``next_seq``：全局递增序号，用于生成稳定的监控分析批次编号。
"""
from __future__ import annotations

import copy
import threading
from contextlib import contextmanager
from typing import Any, Iterator

from app.seed import SEED_ROWS


class PersistError(RuntimeError):
    """模拟落库/事务失败：由失败注入开关触发，服务层捕获后执行回滚。"""


class Store:
    def __init__(self) -> None:
        self._tables: dict[str, list[dict[str, Any]]] = {
            name: [dict(row) for row in rows] for name, rows in SEED_ROWS.items()
        }
        # 非表格类业务实体（监控批次、样本注册表、复核结果等）统一放这里
        self._blobs: dict[str, Any] = {}
        self._seq: dict[str, int] = {}
        # 所有写操作共用一把可重入锁，保证同一面板的并发请求串行化
        self._lock = threading.RLock()

    @property
    def lock(self) -> threading.RLock:
        return self._lock

    def module_names(self) -> list[str]:
        return sorted(self._tables)

    def rows(self, module: str) -> list[dict[str, Any]]:
        return self._tables.setdefault(module, [])

    def find(self, module: str, entry_id: int) -> dict[str, Any] | None:
        for row in self.rows(module):
            if int(row.get("id", 0)) == entry_id:
                return row
        return None

    def overview(self) -> dict[str, object]:
        modules: list[dict[str, object]] = []
        for name in self.module_names():
            rows = self.rows(name)
            modules.append({
                "name": name,
                "created": len(rows),
                "pending": sum(1 for row in rows if row.get("pending")),
                "abnormal": sum(1 for row in rows if row.get("abnormal")),
            })
        cards = [
            {"label": "业务模块", "value": len(modules)},
            {"label": "今日新增", "value": sum(int(item["created"]) for item in modules)},
            {"label": "待处理", "value": sum(int(item["pending"]) for item in modules)},
            {"label": "异常量", "value": sum(int(item["abnormal"]) for item in modules)},
        ]
        return {"cards": cards, "modules": modules}

    # ------------------------------------------------------------------
    # Blob 存储：监控分析批次清单、样本注册表、复核结果
    # ------------------------------------------------------------------
    def blob_get(self, key: str, default: Any = None) -> Any:
        return self._blobs.get(key, default)

    def blob_put(self, key: str, value: Any) -> None:
        self._blobs[key] = value

    def blob_keys(self) -> list[str]:
        return sorted(self._blobs)

    def next_seq(self, name: str) -> int:
        value = self._seq.get(name, 0) + 1
        self._seq[name] = value
        return value

    # ------------------------------------------------------------------
    # 事务：深拷贝快照，落库失败时整体回滚，保证批次不被半写
    # ------------------------------------------------------------------
    def snapshot(self) -> dict[str, Any]:
        return {
            "tables": copy.deepcopy(self._tables),
            "blobs": copy.deepcopy(self._blobs),
            "seq": dict(self._seq),
        }

    def restore(self, snap: dict[str, Any]) -> None:
        self._tables = copy.deepcopy(snap["tables"])
        self._blobs = copy.deepcopy(snap["blobs"])
        self._seq = dict(snap["seq"])

    @contextmanager
    def transaction(self, fail_flag: str | None = None) -> Iterator[None]:
        """在锁内执行一段落库逻辑；``fail_flag`` 命中失败注入时回滚并抛错。

        失败注入约定：``app.config.settings`` 不感知测试，测试直接在 store 上
        写入 ``store.blob_put(fail_flag, True)``，事务提交前检测到即模拟落库失败。
        """
        with self._lock:
            snap = self.snapshot()
            try:
                yield
                if fail_flag and self.blob_get(fail_flag):
                    # 一次性开关：触发后清除，避免污染后续请求
                    self._blobs.pop(fail_flag, None)
                    raise PersistError(f"模拟落库失败：{fail_flag}")
            except Exception:
                self.restore(snap)
                raise


store = Store()
