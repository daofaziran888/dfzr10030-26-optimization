"""HTTP 层验收：启动真实 uvicorn 进程，用标准库 urllib 跑通完整接口链路。

运行方式：python -I tests/verify_http.py
脚本会自行启动/关闭后端，不引入 httpx/pytest 等额外依赖。
"""
from __future__ import annotations

import json
import os
import signal
import subprocess
import sys
import time
import urllib.error
import urllib.request

BACKEND_DIR = "/workspace/backend"
BASE = "http://127.0.0.1:8000"
failures: list[str] = []


def call(method: str, path: str, body: dict | None = None,
         params: dict | None = None) -> tuple[int, dict]:
    url = BASE + path
    if params:
        url += "?" + urllib.parse.urlencode(params)
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(url, data=data, method=method,
                                 headers={"Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(req, timeout=10) as resp:
            return resp.status, json.loads(resp.read().decode())
    except urllib.error.HTTPError as exc:
        return exc.code, json.loads(exc.read().decode())


def check(name: str, cond: bool, detail: str = "") -> None:
    print(f"[{'PASS' if cond else 'FAIL'}] {name}" + (f" :: {detail}" if detail else ""))
    if not cond:
        failures.append(name)


def main() -> None:
    proc = subprocess.Popen(
        [".venv/bin/uvicorn", "app.main:app", "--host", "127.0.0.1", "--port", "8000"],
        cwd=BACKEND_DIR, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
    )
    try:
        for _ in range(50):
            try:
                status, _ = call("GET", "/api/health")
                if status == 200:
                    break
            except OSError:
                time.sleep(0.2)
        else:
            raise RuntimeError("后端服务未在 10 秒内就绪")

        # 兼容旧入口
        status, data = call("POST", "/api/monitor/run",
                            {"values": {"样本范围": 7, "单批容量": 3}})
        check("旧版 /api/monitor/run 仍返回结果项",
              status == 200 and len(data["items"]) == 7, str(status))

        # 提交 MON-0003 超容量样本
        status, data = call("POST", "/api/monitor/batches", {"values": {
            "面板编号": "MON-0003", "样本范围": 12, "单批容量": 5,
            "idempotency_key": "http-1",
        }})
        check("提交批次 200", status == 200, str(status))
        batch_no = data["batch"]["batch_no"]
        check("分批为 3 组且边界正确",
              data["batch"]["progress"]["总分组数"] == 3
              and [c["样本区间"] for c in data["batch"]["chunks"]] == [[1, 5], [6, 10], [11, 12]])

        # 重复提交幂等
        status, data = call("POST", "/api/monitor/batches", {"values": {
            "面板编号": "MON-0003", "样本范围": 12, "单批容量": 5,
            "idempotency_key": "http-1",
        }})
        check("重复提交复用同批次",
              data["reused"] and data["batch"]["batch_no"] == batch_no)

        # 执行一个分组 + 停止 + 回查
        call("POST", f"/api/monitor/batches/{batch_no}/execute", {"values": {"limit": 1}})
        call("POST", f"/api/monitor/batches/{batch_no}/stop", {"values": {}})
        status, view = call("GET", f"/api/monitor/batches/{batch_no}")
        check("停止后可按编号回查且已完成分组保留",
              view["status"] == "已停止" and view["progress"]["已完成分组"] == 1)

        # 继续执行至完成
        call("POST", f"/api/monitor/batches/{batch_no}/execute", {"values": {}})
        _, done = call("GET", f"/api/monitor/batches/{batch_no}")
        check("继续执行后批次完成",
              done["status"] == "已完成" and done["progress"]["已完成分组"] == 3)

        # 同编号回查结果稳定
        _, view_again = call("GET", f"/api/monitor/batches/{batch_no}")
        check("同批次编号重复回查结果一致", view_again == done)

        check("未知批次返回 404",
              call("GET", "/api/monitor/batches/MAB-999999")[0] == 404)

        # 采集事件：冻结策略
        _, fb_data = call("POST", "/api/monitor/batches", {"values": {
            "面板编号": "MON-0003", "样本范围": 6, "单批容量": 3,
            "冲突策略": "freeze", "idempotency_key": "http-freeze",
        }})
        fb_no = fb_data["batch"]["batch_no"]
        call("POST", f"/api/monitor/batches/{fb_no}/execute", {"values": {"limit": 1}})
        status, ev = call("POST", "/api/monitor/events",
                          {"values": {"面板编号": "MON-0003", "事件数量": 2}})
        check("采集事件接口受理并按冻结策略处理",
              status == 200 and "下一批次" in ev["处理方式"])
        _, events = call("GET", "/api/monitor/events", {"panel": "MON-0003"})
        check("事件清单可按面板回查", events["total"] >= 1)

        # 报告复核
        status, rv = call("POST", "/api/report/batch-reviews",
                          {"values": {"batch_no": batch_no}})
        check("报告按批次生成待复核结果",
              status == 200 and rv["review"]["复核状态"] == "待复核")
        _, rv2 = call("POST", "/api/report/batch-reviews",
                      {"values": {"batch_no": batch_no}})
        check("复核重复提交幂等", rv2["reused"])
        status, rv_get = call("GET", f"/api/report/batch-reviews/{batch_no}")
        check("复核结果可按批次编号回查", status == 200)
        status, rv_list = call("GET", "/api/report/batch-reviews", {"panel": "MON-0003"})
        check("复核清单可按面板回查", status == 200 and rv_list["total"] >= 1)
        check("复核成功率与平均延迟已汇总",
              0 < rv_get["成功率"] <= 1 and rv_get["平均延迟"] > 0)

        # 落库失败回滚的 HTTP 表现：_fail_flag 由服务层在事务内自行注入并消费
        status, fail_r = call("POST", "/api/monitor/batches", {"values": {
            "面板编号": "MON-0003", "样本范围": 3, "单批容量": 5,
            "idempotency_key": "http-rollback", "_fail_flag": "fail:http",
        }})
        check("落库失败返回 500 且提示已回滚",
              status == 500 and "回滚" in fail_r["detail"], str(status))
        _, listing = call("GET", "/api/monitor/batches")
        check("回滚后未产生新批次",
              all(b["idempotency_key"] != "http-rollback" for b in listing["items"]))

        # 面板不存在
        status, missing = call("POST", "/api/monitor/batches", {"values": {
            "面板编号": "MON-9999", "样本范围": 3,
        }})
        check("未知面板返回 404", status == 404)

    finally:
        proc.send_signal(signal.SIGINT)
        try:
            proc.wait(timeout=5)
        except subprocess.TimeoutExpired:
            proc.kill()

    print()
    if failures:
        print(f"HTTP 验收失败 {len(failures)} 项：{failures}")
        sys.exit(1)
    print("HTTP 层全部验收项通过")


if __name__ == "__main__":
    main()
