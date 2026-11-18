"""监控分析分批执行的端到端验收脚本（直接调用服务层，不依赖第三方库）。

覆盖：
1. MON-0003 超过单批容量样本的分批边界；
2. 已完成分组不重算、停止后续跑只处理未完成分组；
3. 重复提交幂等、历史批次保留原状态、同批次编号回查结果一致；
4. 落库失败事务回滚；
5. 两种采集事件冲突策略均不混入两个样本版本；
6. 采集报告按批次汇总成功率/平均延迟，复核幂等。
"""
from __future__ import annotations

import sys

sys.path.insert(0, "/workspace/backend")

from app.services.monitor_batch import (  # noqa: E402
    STRATEGY_FREEZE,
    STRATEGY_NEW_BATCH,
    batch_service,
)
from app.services.report import ReportService  # noqa: E402
from app.store import PersistError, store  # noqa: E402

failures: list[str] = []


def check(name: str, cond: bool, detail: str = "") -> None:
    status = "PASS" if cond else "FAIL"
    print(f"[{status}] {name}" + (f" :: {detail}" if detail else ""))
    if not cond:
        failures.append(name)


# ---------------------------------------------------------------- 1. 分批边界
submitted = batch_service.submit_analysis({
    "面板编号": "MON-0003",
    "样本范围": 12,
    "单批容量": 5,
    "idempotency_key": "acceptance-1",
})
batch_no = submitted["batch"]["batch_no"]
manifest = submitted["batch"]
check("提交返回新批次而非复用", not submitted["reused"])
check("12 个样本按容量 5 拆成 3 个分组", manifest["progress"]["总分组数"] == 3,
      str(manifest["progress"]))
check("分批边界为 1-5 / 6-10 / 11-12",
      [c["样本区间"] for c in manifest["chunks"]] == [[1, 5], [6, 10], [11, 12]],
      str([c["样本区间"] for c in manifest["chunks"]]))
check("批次内所有分组使用同一样本版本",
      len({c["sample_version"] for c in manifest["chunks"]}) == 1)
check("生成了监控指标快照",
      set(["成功率", "平均延迟", "snapshot_at"]).issubset(manifest["metric_snapshot"]))

# ---------------------------------------------------------------- 2. 分组执行与不重算
out1 = batch_service.execute_batch(batch_no, limit=1)
check("首轮只执行 1 个分组", out1["processed"] == 1)
m1 = batch_service.get_manifest(batch_no)
first_chunk = m1["chunks"][0]
check("第一个分组已完成", first_chunk["status"] == "已完成")
frozen_result = first_chunk["result"]
out2 = batch_service.execute_batch(batch_no, limit=1)
check("再次执行跳过已完成分组，只处理下一个", out2["processed"] == 1)
m2 = batch_service.get_manifest(batch_no)
check("已完成分组结果未被重算", m2["chunks"][0]["result"] == frozen_result)
check("当前进度 2/3", m2["progress"]["已完成分组"] == 2)

# 停止
stopped = batch_service.stop_batch(batch_no)
m3 = batch_service.get_manifest(batch_no)
check("停止后批次状态为已停止", m3["status"] == "已停止" and stopped["changed"])
check("剩余分组标记为已停止", m3["chunks"][2]["status"] == "已停止")
check("已完成分组保留", m3["chunks"][0]["result"] == frozen_result)

# 继续执行
resumed = batch_service.execute_batch(batch_no)
m4 = batch_service.get_manifest(batch_no)
check("继续执行只处理未完成分组（1 个）", resumed["processed"] == 1, str(resumed["processed"]))
check("继续后批次已完成", m4["status"] == "已完成")
check("全部 3 个分组完成", m4["progress"]["已完成分组"] == 3)
check("完成后继续执行幂等返回 0", batch_service.execute_batch(batch_no)["processed"] == 0)

# ---------------------------------------------------------------- 3. 幂等与回查一致
again = batch_service.submit_analysis({
    "面板编号": "MON-0003", "样本范围": 12, "单批容量": 5,
    "idempotency_key": "acceptance-1",
})
check("同幂等键重复提交只生效一次", again["reused"] and again["batch"]["batch_no"] == batch_no)

# 不同参数但同面板仍有未终结批次时也复用（当前批次已完成，所以这里显式建第二个批次验证历史保留）
second = batch_service.submit_analysis({
    "面板编号": "MON-0003", "样本范围": 7, "单批容量": 5,
    "idempotency_key": "acceptance-2",
})
second_no = second["batch"]["batch_no"]
check("新提交创建独立批次", second_no != batch_no and not second["reused"])
batch_service.execute_batch(second_no, limit=1)
batch_service.stop_batch(second_no)
# 不带幂等键再提交：存在已停止的活动批次，应复用
reuse_active = batch_service.submit_analysis({
    "面板编号": "MON-0003", "样本范围": 7, "单批容量": 5,
    "idempotency_key": "acceptance-3",
})
check("已停止批次仍属活动批次，重复请求复用同一批次",
      reuse_active["reused"] and reuse_active["batch"]["batch_no"] == second_no)

# 历史批次状态保留
history = batch_service.get_manifest(batch_no)
check("历史批次保留已完成状态", history["status"] == "已完成")
check("历史批次分组结果仍可按同一编号回查",
      history["chunks"][0]["result"] == frozen_result)

# 同编号两次回查内容一致（监控页/报告页重新进入看到相同结果）
view_a = batch_service.get_manifest(batch_no)
view_b = batch_service.get_manifest(batch_no)
check("同批次编号重复回查结果一致", view_a == view_b)

# ---------------------------------------------------------------- 4. 落库失败回滚
before = store.snapshot()
store.blob_put("fail:submit", True)
rolled_back = False
try:
    batch_service.submit_analysis({
        "面板编号": "MON-0003", "样本范围": 3, "单批容量": 5,
        "idempotency_key": "will-rollback", "_fail_flag": "fail:submit",
    })
except PersistError:
    rolled_back = True
check("失败注入触发 PersistError", rolled_back)
# 失败注入开关本身在事务提交时被消费，比较时剔除该一次性键
after_blobs = {k: v for k, v in store.snapshot()["blobs"].items() if k != "fail:submit"}
before_blobs = {k: v for k, v in before["blobs"].items() if k != "fail:submit"}
check("事务回滚后仓库恢复到提交前状态", after_blobs == before_blobs)

# 执行失败回滚
store.blob_put("fail:exec", True)
exec_rollback = False
try:
    batch_service.execute_batch(second_no, fail_flag="fail:exec")
except PersistError:
    exec_rollback = True
check("执行落库失败抛错并回滚", exec_rollback)
still_stopped = batch_service.get_manifest(second_no)
check("回滚后原批次维持已停止状态", still_stopped["status"] == "已停止")

# 收尾：把第二个批次跑完，避免同面板活动批次复用规则影响后续策略验收
batch_service.execute_batch(second_no)
check("前置批次收尾后状态为已完成",
      batch_service.get_manifest(second_no)["status"] == "已完成")

# ---------------------------------------------------------------- 5a. 冲突策略一：冻结
frozen_batch = batch_service.submit_analysis({
    "面板编号": "MON-0003", "样本范围": 6, "单批容量": 3,
    "冲突策略": STRATEGY_FREEZE, "idempotency_key": "freeze-case",
})
check("冻结策略验收使用全新批次", not frozen_batch["reused"])
fb_no = frozen_batch["batch"]["batch_no"]
batch_service.execute_batch(fb_no, limit=1)
fb_version_before = batch_service.get_manifest(fb_no)["sample_version"]
event1 = batch_service.record_event({"面板编号": "MON-0003", "事件数量": 4})
fb_after = batch_service.get_manifest(fb_no)
check("冻结策略：新事件不改变当前批次样本版本",
      fb_after["sample_version"] == fb_version_before)
check("冻结策略：当前批次分组数与样本集合不变",
      fb_after["sample_count"] == frozen_batch["batch"]["sample_count"]
      and fb_after["progress"]["总分组数"] == frozen_batch["batch"]["progress"]["总分组数"])
check("冻结策略：事件登记为进入下一批次",
      "下一批次" in event1["处理方式"] and fb_no in event1["affected_batches"])
check("冻结策略：当前批次每个分组仍只有单一样本版本",
      all(len({c["sample_version"] for c in fb_after["chunks"]}) == 1 for _ in [0]))

# ---------------------------------------------------------------- 5b. 冲突策略二：新样本集合
# 先完成冻结批次，保证新提交不会被同面板活动批次复用规则拦截
batch_service.execute_batch(fb_no)
nb = batch_service.submit_analysis({
    "面板编号": "MON-0003", "样本范围": 6, "单批容量": 3,
    "冲突策略": STRATEGY_NEW_BATCH, "idempotency_key": "newbatch-case",
})
check("新批次策略验收使用全新批次", not nb["reused"])
nb_no = nb["batch"]["batch_no"]
batch_service.execute_batch(nb_no, limit=1)
event2 = batch_service.record_event({"面板编号": "MON-0003", "事件数量": 3})
pred = batch_service.get_manifest(nb_no)
succ_no = pred["successor_batch_no"]
check("新批次策略：原批次被停止并产生后继批次", succ_no is not None and pred["status"] == "已停止")
succ = batch_service.get_manifest(succ_no)
check("新批次策略：后继批次使用更新的样本版本",
      succ["sample_version"] > pred["sample_version"])
check("新批次策略：后继批次携带原批次已完成分组",
      len(succ["carried_chunks"]) == 1 and succ["carried_chunks"][0]["batch_no"] == nb_no)
check("新批次策略：原批次已完成分组结果保留",
      pred["chunks"][0]["status"] == "已完成" and pred["chunks"][0]["result"] is not None)
check("新批次策略：后继批次自身不混样本版本",
      len({c["sample_version"] for c in succ["chunks"]}) == 1)
check("新批次策略：事件同时关联原批次与后继批次",
      event2["affected_batches"] == [nb_no, succ_no])

# ---------------------------------------------------------------- 6. 报告复核汇总
report_service = ReportService()
review_out = report_service.summarize_batch({"batch_no": batch_no})
review = review_out["review"]
check("报告读取监控批次清单生成待复核结果", review["复核状态"] == "待复核")
check("复核覆盖全部 3 个已完成分组", review["已完成分组"] == 3)
check("复核汇总样本数与批次一致（12）", review["总样本数"] == 12, str(review["总样本数"]))

# 手工核对加权汇总
finished = [c for c in batch_service.get_manifest(batch_no)["chunks"] if c["status"] == "已完成"]
exp_success = sum(c["result"]["aggregated"]["成功率"] * c["样本数"]
                  for c in finished) / 12
exp_latency = sum(c["result"]["aggregated"]["平均延迟"] * c["样本数"]
                  for c in finished) / 12
check("复核成功率与分组加权结果一致", abs(review["成功率"] - round(exp_success, 4)) < 1e-9)
check("复核平均延迟与分组加权结果一致", abs(review["平均延迟"] - round(exp_latency, 2)) < 1e-9)

review_again = report_service.summarize_batch({"batch_no": batch_no})
check("同批次复核重复提交只生效一次",
      review_again["reused"] and review_again["review"]["复核编号"] == review["复核编号"])

# 后继批次复核应包含携带分组（先让后继批次至少完成一个分组）
batch_service.execute_batch(succ_no, limit=1)
succ_review = report_service.summarize_batch({"batch_no": succ_no})
sources = {row["来源批次"] for row in succ_review["review"]["明细"]}
check("后继批次复核纳入前序保留分组", nb_no in sources and succ_no in sources, str(sources))

# 复核落库失败回滚
store.blob_put("fail:review", True)
review_rollback = False
try:
    report_service.summarize_batch({"batch_no": fb_no}, fail_flag="fail:review")
except PersistError:
    review_rollback = True
check("复核落库失败时事务回滚", review_rollback)
check("回滚后未留下半成品复核记录",
      report_service.get_batch_review(fb_no) is None)

# ---------------------------------------------------------------- 7. 清单总览
listing = batch_service.list_batches("MON-0003")
check("批次清单可按面板回查全部批次",
      {b["batch_no"] for b in listing} >= {batch_no, second_no, fb_no, nb_no, succ_no})
reviews_listing = report_service.list_batch_reviews("MON-0003")
check("报告页可按面板回查复核清单", len(reviews_listing) >= 2)

print()
if failures:
    print(f"验收失败 {len(failures)} 项：{failures}")
    sys.exit(1)
print("全部验收项通过")
