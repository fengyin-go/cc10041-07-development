"""数据采样领域服务：执行采样走归属清单，旧配置只存表名也能按可信域取数。"""
from __future__ import annotations

from typing import Any

from app.sampling import ledger
from app.services.base import BaseModuleService


class DataSampleService(BaseModuleService):
    MODULE = "data_sample"
    ENTITY = "采样配置"
    OPERATION_TERM = "导出"
    LIST_FIELDS = ["配置名称", "采样比例", "采样方式", "目标表", "采样状态"]
    REQUIRED_FIELDS = ["配置名称", "采样比例", "采样方式"]
    STATUS_ORDER = ["待执行", "已执行", "已过期"]
    ACTION_RULES = {"执行采样": "已执行", "过期配置": "已过期"}
    NEGATIVE_ACTIONS = []

    def create_entry(
        self,
        values: dict[str, Any],
        context: Any | None = None,
    ) -> tuple[dict[str, Any] | None, list[str]]:
        entry, missing = super().create_entry(values, context)
        if entry is not None:
            entry["采样状态"] = "待执行"
        return entry, missing

    def run_action(
        self,
        entry_id: int,
        action: str,
        context: Any | None = None,
    ) -> tuple[dict[str, Any] | None, str]:
        entry = self.get_entry(entry_id)
        if entry is None:
            return None, f"{self.ENTITY} {entry_id} 不存在或已归档"
        if action == "过期配置":
            # 过期只是配置生命周期动作，不读取任何分区；取消该配置的排队请求。
            entry["status"] = self.ACTION_RULES[action]
            entry["pending"] = False
            entry["采样状态"] = "已过期，不再接受采样"
            cancelled = ledger().cancel_requests_for_config(entry_id)
            suffix = f"；已撤销 {cancelled} 笔排队采样" if cancelled else ""
            return entry, f"{self.ENTITY}已过期{suffix}"
        if action in self.ACTION_RULES:
            # 配置只是排队入口：真正的取数在采样请求执行时按归属清单重算。
            result = ledger().queue_request(config=entry, context=context)
            return entry, result["message"]
        return super().run_action(entry_id, action, context)

    def domain_snapshot(self) -> dict[str, Any]:
        snapshot = super().domain_snapshot()
        mine = [r for r in ledger().requests]
        snapshot["sampling_requests"] = len(mine)
        snapshot["delivered_requests"] = sum(1 for r in mine if r.delivered)
        snapshot["ownership_manifest_version"] = ledger().manifest_version
        return snapshot
