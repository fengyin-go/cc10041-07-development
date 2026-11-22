"""Iceberg表领域服务：登记时校验归属，让采样按业务域追到对应分区。"""
from __future__ import annotations

from typing import Any

from app.sampling import ledger
from app.services.base import BaseModuleService
from app.services.errors import BusinessRuleError
from app.store import store
from app.trusted import canonical_domain


class IcebergTableService(BaseModuleService):
    MODULE = "iceberg_table"
    ENTITY = "Iceberg表"
    OPERATION_TERM = "存储"
    LIST_FIELDS = ["表名", "所属业务域", "存储格式", "分区定义", "当前快照数"]
    REQUIRED_FIELDS = ["表名", "所属业务域", "存储格式"]
    STATUS_ORDER = ["创建中", "已注册", "已下线"]
    ACTION_RULES = {"注册表": "已注册", "下线表": "已下线"}
    NEGATIVE_ACTIONS = ["下线表"]

    def _check_registration(
        self,
        table: str,
        domain: str,
        context: Any | None,
        *,
        ignore_id: int | None = None,
    ) -> None:
        """表名只是名字：归属必须落到业务域，且不能与可信调用域冲突。"""
        domain = canonical_domain(domain) or ""
        if context is not None and getattr(context, "usable", False):
            trusted_domain = canonical_domain(context.domain)
            if trusted_domain != domain:
                raise BusinessRuleError(
                    f"所属业务域「{domain}」与可信调用域「{trusted_domain}」不一致，"
                    "未建立归属登记"
                )
        for row in store.rows(self.MODULE):
            if ignore_id is not None and int(row.get("id", 0)) == ignore_id:
                continue
            if (
                str(row.get("表名") or "") == table
                and canonical_domain(str(row.get("所属业务域") or "")) == domain
            ):
                raise BusinessRuleError(
                    f"{domain}域已登记表「{table}」，同名表请按业务域分别登记"
                )

    def create_entry(
        self,
        values: dict[str, Any],
        context: Any | None = None,
    ) -> tuple[dict[str, Any] | None, list[str]]:
        table = str(values.get("表名") or "").strip()
        domain = canonical_domain(str(values.get("所属业务域") or "")) or ""
        if table and domain:
            self._check_registration(table, domain, context)
        entry, missing = super().create_entry(values, context)
        if entry is not None:
            # 归属清单只认「表名 + 所属业务域」，同名表在两个域各占一条。
            actual_domain = canonical_domain(str(entry.get("所属业务域") or ""))
            if actual_domain:
                entry["所属业务域"] = actual_domain
                ledger().ensure_claim(
                    table=str(entry.get("表名") or ""),
                    domain=actual_domain,
                    owner=getattr(context, "principal", None),
                    note="随Iceberg表登记自动建立归属",
                )
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
        if action == "注册表":
            domain = canonical_domain(str(entry.get("所属业务域") or "")) or ""
            self._check_registration(
                str(entry.get("表名") or ""), domain, context, ignore_id=entry_id
            )
            ledger().ensure_claim(
                table=str(entry.get("表名") or ""),
                domain=domain,
                note="随Iceberg表注册建立归属",
            )
        return super().run_action(entry_id, action, context)

    def domain_snapshot(self) -> dict[str, Any]:
        snapshot = super().domain_snapshot()
        names: dict[str, list[str]] = {}
        for claim in ledger().claims:
            names.setdefault(claim.table, []).append(claim.domain)
        snapshot["shared_names"] = {
            table: sorted(set(domains))
            for table, domains in names.items()
            if len(set(domains)) > 1
        }
        snapshot["ownership_claims"] = len(ledger().claims)
        return snapshot
