"""采样归属清单与按域采样：表名只是名字，业务域身份来自可信调用上下文。

设计要点：
- 归属清单（manifest）按「表名 + 所属业务域」登记，同名表在不同业务域互不覆盖。
- 采样请求排队时只记录可信域身份与归属清单快照，不读取任何分区行。
- 交付前重新读取归属清单，只取「归属已确认且分区已核准」的数据；
  排队后被撤回的分区即使曾经核准过也不会出现在交付结果里。
- 归属不明时保留核对依据（basis），不把任意分区交给抽样人。
"""
from __future__ import annotations

import math
import threading
import time
from dataclasses import dataclass, field
from typing import Any, Iterable

from app.trusted import TrustedContext, canonical_domain

PENDING = "待核对"
CONFIRMED = "已确认"
REJECTED = "已驳回"

DRAFT = "待核对"
APPROVED = "已核准"
REVOKED = "已撤回"

QUEUED = "排队中"
DELIVERED = "已交付"
NOT_DELIVERED = "未交付"
CANCELLED = "已撤销"


def _now() -> int:
    return int(time.time() * 1000)


def _text(value: Any) -> str:
    return str(value or "").strip()


@dataclass
class SampleRow:
    row_id: str
    values: dict[str, Any] = field(default_factory=dict)

    def view(self) -> dict[str, Any]:
        return {"row_id": self.row_id, "values": dict(self.values)}


@dataclass
class Partition:
    id: int
    name: str
    status: str = DRAFT
    rows: list[SampleRow] = field(default_factory=list)
    note: str = ""
    updated_by: str | None = None
    updated_at: int = field(default_factory=_now)

    @property
    def row_ids(self) -> list[str]:
        return [row.row_id for row in self.rows]

    def view(self, *, include_rows: bool = False) -> dict[str, Any]:
        payload: dict[str, Any] = {
            "partition_id": self.id,
            "name": self.name,
            "status": self.status,
            "sample_count": len(self.rows),
            "note": self.note,
            "updated_by": self.updated_by,
            "updated_at": self.updated_at,
        }
        if include_rows:
            payload["rows"] = [row.view() for row in self.rows]
        return payload


@dataclass
class OwnershipClaim:
    id: int
    table: str
    domain: str
    status: str = PENDING
    owner: str | None = None
    grant: str | None = None
    note: str = ""
    basis: dict[str, Any] = field(default_factory=dict)
    partitions: list[Partition] = field(default_factory=list)
    history: list[dict[str, Any]] = field(default_factory=list)

    def partition(self, partition_id: int) -> Partition | None:
        return next((item for item in self.partitions if item.id == partition_id), None)

    def view(self, *, include_rows: bool = False) -> dict[str, Any]:
        return {
            "claim_id": self.id,
            "table": self.table,
            "domain": self.domain,
            "ownership_status": self.status,
            "owner": self.owner,
            "grant": self.grant,
            "note": self.note,
            "basis": dict(self.basis),
            "partitions": [
                item.view(include_rows=include_rows) for item in self.partitions
            ],
            "confirmed_partitions": [
                item.id for item in self.partitions if item.status == APPROVED
            ],
        }


@dataclass
class SamplingRequest:
    id: int
    config_id: int | None
    config_name: str
    target_table: str
    sample_ratio: float
    sample_method: str
    domain: str | None
    principal: str | None
    grant: str | None
    status: str
    reason: str
    basis: dict[str, Any]
    queued_at: int = field(default_factory=_now)
    executed_at: int | None = None
    rows: list[dict[str, Any]] = field(default_factory=list)
    lineage: list[dict[str, Any]] = field(default_factory=list)
    manifest_version: int = 0
    history: list[dict[str, Any]] = field(default_factory=list)
    blocked: bool = False

    @property
    def delivered(self) -> bool:
        return self.status == DELIVERED

    def view(self, *, include_rows: bool = True) -> dict[str, Any]:
        return {
            "request_id": self.id,
            "config_id": self.config_id,
            "config_name": self.config_name,
            "target_table": self.target_table,
            "sample_ratio": self.sample_ratio,
            "sample_method": self.sample_method,
            "domain": self.domain,
            "principal": self.principal,
            "grant": self.grant,
            "status": self.status,
            "delivered": self.delivered,
            "blocked": self.blocked,
            "reason": self.reason,
            # 行数/行 ID 也是交付内容本身：调用方不能读时一并隐藏。
            "row_count": len(self.rows) if include_rows else None,
            "row_ids": [row.get("row_id") for row in self.rows] if include_rows else [],
            "rows": list(self.rows) if include_rows else [],
            "lineage": list(self.lineage) if include_rows else [],
            "manifest_version": self.manifest_version,
            "basis": dict(self.basis),
            "queued_at": self.queued_at,
            "executed_at": self.executed_at,
            "history": list(self.history) if include_rows else [],
            "rows_visible": include_rows,
        }


class SamplingLedger:
    """内存归属清单与采样请求队列。"""

    def __init__(self) -> None:
        self._lock = threading.RLock()
        self.manifest_version = 0
        self.claims: list[OwnershipClaim] = []
        self.requests: list[SamplingRequest] = []
        self._claim_seq = 0
        self._partition_seq = 0
        self._request_seq = 0
        self.reset_demo()

    # ------------------------------------------------------------------
    # 基础登记
    # ------------------------------------------------------------------
    def _bump(self, reason: str, actor: str | None = None) -> None:
        """归属清单有变更时递增版本号，交付时用它说明依据的时间点。"""
        self.manifest_version += 1

    def _next_claim_id(self) -> int:
        self._claim_seq += 1
        return self._claim_seq

    def _next_partition_id(self) -> int:
        self._partition_seq += 1
        return self._partition_seq

    def _next_request_id(self) -> int:
        self._request_seq += 1
        return self._request_seq

    def find_claim(self, claim_id: int) -> OwnershipClaim | None:
        return next((c for c in self.claims if c.id == claim_id), None)

    def claims_for(
        self, table: str | None = None, domain: str | None = None
    ) -> list[OwnershipClaim]:
        rows = list(self.claims)
        if table:
            rows = [c for c in rows if c.table == table]
        if domain:
            rows = [c for c in rows if canonical_domain(c.domain) == canonical_domain(domain)]
        return rows

    def find_partition(self, partition_id: int) -> tuple[OwnershipClaim, Partition] | None:
        for claim in self.claims:
            part = claim.partition(partition_id)
            if part is not None:
                return claim, part
        return None

    def ensure_claim(
        self,
        *,
        table: str,
        domain: str,
        owner: str | None = None,
        note: str = "",
        grant: str | None = None,
    ) -> OwnershipClaim:
        table = _text(table)
        domain = canonical_domain(domain) or _text(domain)
        for claim in self.claims:
            if claim.table == table and claim.domain == domain:
                if owner and not claim.owner:
                    claim.owner = owner
                if grant and not claim.grant:
                    claim.grant = grant
                return claim
        claim = OwnershipClaim(
            id=self._next_claim_id(),
            table=table,
            domain=domain,
            status=PENDING,
            owner=owner,
            grant=grant,
            note=note,
        )
        self.claims.append(claim)
        self._bump("登记归属")
        return claim

    def add_partition(
        self,
        claim: OwnershipClaim,
        *,
        name: str,
        rows: Iterable[dict[str, Any] | str],
        status: str = DRAFT,
        note: str = "",
    ) -> Partition:
        sample_rows: list[SampleRow] = []
        for item in rows:
            if isinstance(item, str):
                sample_rows.append(SampleRow(row_id=item, values={"id": item}))
            elif isinstance(item, dict):
                row_id = _text(item.get("row_id") or item.get("id") or item.get("row_key"))
                if not row_id:
                    row_id = f"row-{len(sample_rows) + 1}"
                values = dict(item.get("values") or {}) or {
                    key: value for key, value in item.items() if key not in {"row_id"}
                }
                sample_rows.append(SampleRow(row_id=row_id, values=values))
        part = Partition(
            id=self._next_partition_id(),
            name=_text(name) or f"partition-{self._partition_seq}",
            status=status,
            rows=sample_rows,
            note=note,
        )
        claim.partitions.append(part)
        self._bump("登记分区")
        return part

    # ------------------------------------------------------------------
    # 归属核对
    # ------------------------------------------------------------------
    def add_partition_by_id(
        self,
        claim_id: int,
        *,
        name: str,
        rows: Iterable[dict[str, Any] | str],
        status: str = DRAFT,
        note: str = "",
        context: TrustedContext | None = None,
    ) -> dict[str, Any]:
        claim = self.find_claim(claim_id)
        if claim is None:
            return {"ok": False, "message": f"归属登记 {claim_id} 不存在"}
        if context is None or self._claim_actor(context, claim) is None:
            return {
                "ok": False,
                "message": "新增分区必须由该业务域的可信身份发起，已保留核对依据",
                "claim": claim.view(),
            }
        # 只有该域可信身份才能把分区放进可读取状态。
        if status == APPROVED:
            if claim.status != CONFIRMED:
                return {
                    "ok": False,
                    "message": f"{claim.domain}域 {claim.table} 的归属尚未确认，不能核准分区",
                    "claim": claim.view(),
                }
        part = self.add_partition(claim, name=name, rows=rows, status=status, note=note)
        return {
            "ok": True,
            "message": f"分区 {part.name} 已登记",
            "claim": claim.view(),
            "partition": part.view(include_rows=True),
        }

    def _claim_actor(self, context: TrustedContext, claim: OwnershipClaim) -> str | None:
        if not context.usable:
            return None
        if canonical_domain(context.domain) != canonical_domain(claim.domain):
            return None
        return context.principal or context.domain

    def confirm_claim(
        self, claim_id: int, context: TrustedContext, *, grant: str | None = None
    ) -> dict[str, Any]:
        claim = self.find_claim(claim_id)
        if claim is None:
            return {"ok": False, "message": f"归属登记 {claim_id} 不存在", "claim": None}
        actor = self._claim_actor(context, claim)
        if actor is None:
            return {
                "ok": False,
                "message": "可信调用上下文与归属登记的业务域不一致，已保留核对依据",
                "claim": claim.view(),
                "basis": {
                    "trusted": context.evidence(),
                    "claim": claim.view(),
                    "reason": "确认归属必须由该业务域的可信身份发起",
                },
            }
        if claim.grant and _text(grant or context.grant) != claim.grant:
            return {
                "ok": False,
                "message": "归属凭据未通过核验，已保留核对依据",
                "claim": claim.view(),
                "basis": {
                    "trusted": context.evidence(),
                    "claim": claim.view(),
                    "reason": "归属凭据不匹配",
                },
            }
        claim.status = CONFIRMED
        if not claim.owner:
            claim.owner = context.principal
        claim.history.append(
            {
                "at": _now(),
                "action": "确认归属",
                "actor": actor,
                "domain": claim.domain,
            }
        )
        self._bump("确认归属", actor)
        return {"ok": True, "message": f"{claim.domain}域 {claim.table} 的归属已确认", "claim": claim.view()}

    def reject_claim(self, claim_id: int, context: TrustedContext, reason: str = "") -> dict[str, Any]:
        claim = self.find_claim(claim_id)
        if claim is None:
            return {"ok": False, "message": f"归属登记 {claim_id} 不存在", "claim": None}
        actor = self._claim_actor(context, claim)
        if actor is None:
            return {
                "ok": False,
                "message": "可信调用上下文与归属登记的业务域不一致，已保留核对依据",
                "claim": claim.view(),
                "basis": {"trusted": context.evidence(), "claim": claim.view()},
            }
        claim.status = REJECTED
        claim.history.append(
            {"at": _now(), "action": "驳回归属", "actor": actor, "note": reason}
        )
        self._bump("驳回归属", actor)
        return {"ok": True, "message": f"{claim.domain}域 {claim.table} 的归属已驳回", "claim": claim.view()}

    def approve_partition(
        self, partition_id: int, context: TrustedContext, *, note: str = ""
    ) -> dict[str, Any]:
        found = self.find_partition(partition_id)
        if found is None:
            return {"ok": False, "message": f"分区 {partition_id} 不存在"}
        claim, part = found
        actor = self._claim_actor(context, claim)
        if actor is None:
            return {
                "ok": False,
                "message": "可信调用上下文与分区所属业务域不一致，未变更分区状态",
                "claim": claim.view(),
            }
        if claim.status != CONFIRMED:
            return {
                "ok": False,
                "message": f"{claim.domain}域 {claim.table} 的归属尚未确认，不能核准分区",
                "claim": claim.view(),
            }
        part.status = APPROVED
        part.updated_by = actor
        part.updated_at = _now()
        part.note = note or part.note
        claim.history.append(
            {
                "at": _now(),
                "action": "核准分区",
                "actor": actor,
                "partition": part.name,
                "partition_id": part.id,
            }
        )
        self._bump("核准分区", actor)
        return {
            "ok": True,
            "message": f"分区 {part.name} 已核准",
            "claim": claim.view(),
            "partition": part.view(),
        }

    def revoke_partition(
        self, partition_id: int, context: TrustedContext, *, note: str = ""
    ) -> dict[str, Any]:
        found = self.find_partition(partition_id)
        if found is None:
            return {"ok": False, "message": f"分区 {partition_id} 不存在"}
        claim, part = found
        actor = self._claim_actor(context, claim)
        if actor is None:
            return {
                "ok": False,
                "message": "可信调用上下文与分区所属业务域不一致，未变更分区状态",
                "claim": claim.view(),
            }
        part.status = REVOKED
        part.updated_by = actor
        part.updated_at = _now()
        part.note = note or part.note
        claim.history.append(
            {
                "at": _now(),
                "action": "撤回分区",
                "actor": actor,
                "partition": part.name,
                "partition_id": part.id,
            }
        )
        self._bump("撤回分区", actor)
        return {
            "ok": True,
            "message": f"分区 {part.name} 已撤回，后续采样不会读取",
            "claim": claim.view(),
            "partition": part.view(),
        }

    # ------------------------------------------------------------------
    # 查询范围
    # ------------------------------------------------------------------
    def _basis(
        self,
        *,
        table: str,
        domain: str | None,
        context: TrustedContext,
        reason: str,
        claims: list[OwnershipClaim] | None = None,
    ) -> dict[str, Any]:
        """核对依据：本域给出分区状态，他域只保留归属层面的信息。"""
        own = canonical_domain(domain)
        claim_basis: list[dict[str, Any]] = []
        for claim in claims or []:
            visible = own is not None and claim.domain == own
            item: dict[str, Any] = {
                "claim_id": claim.id,
                "domain": claim.domain,
                "ownership_status": claim.status,
                "projection": "own-domain" if visible else "other-domain",
            }
            if visible:
                item["partitions"] = [
                    {
                        "partition_id": part.id,
                        "name": part.name,
                        "status": part.status,
                        "sample_count": len(part.rows),
                    }
                    for part in claim.partitions
                ]
            else:
                item["partition_count"] = len(claim.partitions)
            claim_basis.append(item)
        return {
            "manifest_version": self.manifest_version,
            "table": table,
            "domain": own,
            "reason": reason,
            "trusted": context.evidence(),
            "claims": claim_basis,
        }

    def table_scope(self, table: str, context: TrustedContext) -> dict[str, Any]:
        """按可信域展示同名表在各自业务域的范围；他域只给归属状态。"""
        table = _text(table)
        all_claims = self.claims_for(table=table)
        own_domain = canonical_domain(context.domain) if context.usable else None
        own_claims = [c for c in all_claims if c.domain == own_domain] if own_domain else []
        other_claims = [c for c in all_claims if c.domain != own_domain]

        views: list[dict[str, Any]] = []
        for claim in all_claims:
            visible = own_domain is not None and claim.domain == own_domain
            payload = {
                "domain": claim.domain,
                "table": claim.table,
                "visible": visible,
                "ownership_status": claim.status,
                "owner": claim.owner if visible else None,
                "claim_id": claim.id if visible else None,
                "partitions": (
                    [
                        {
                            "partition_id": part.id,
                            "name": part.name,
                            "status": part.status,
                            "sample_count": len(part.rows),
                        }
                        for part in claim.partitions
                    ]
                    if visible
                    else []
                ),
                "note": (
                    "当前可信身份可核对"
                    if visible
                    else "同名表在其他业务域登记，需由该域可信身份核对"
                ),
            }
            views.append(payload)

        reason = ""
        if not context.usable:
            reason = "缺少可信调用上下文，仅保留核对依据，不展示分区样本"
        elif not own_claims:
            reason = f"可信域 {own_domain} 没有表 {table} 的归属登记；表名本身不能作为读取凭据"

        return {
            "table": table,
            "manifest_version": self.manifest_version,
            "trusted": context.evidence(),
            "authorized": bool(context.usable and own_claims),
            "domains": views,
            "same_name_domains": [c.domain for c in other_claims],
            "reason": reason,
            "basis": self._basis(
                table=table,
                domain=own_domain,
                context=context,
                reason=reason or "按可信域展示归属范围",
                claims=all_claims,
            ),
        }

    def legacy_config_scope(self, config: dict[str, Any], context: TrustedContext) -> dict[str, Any]:
        """旧采样配置只存表名，这里按可信域解析两域范围，绝不回退到他域。"""
        table = _text(config.get("目标表"))
        scope = self.table_scope(table, context)
        return {
            "config_id": config.get("id"),
            "config_name": config.get("配置名称"),
            "target_table": table,
            "sample_ratio": config.get("采样比例"),
            "sample_method": config.get("采样方式"),
            "has_stored_domain": any(
                key in config for key in ("所属业务域", "业务域", "domain")
            ),
            **scope,
        }

    # ------------------------------------------------------------------
    # 采样请求
    # ------------------------------------------------------------------
    def queue_request(
        self,
        *,
        config: dict[str, Any],
        context: TrustedContext,
        sample_ratio: float | None = None,
        sample_method: str | None = None,
    ) -> dict[str, Any]:
        """排队只记录可信域身份与清单快照；归属不明时不创建可交付请求。"""
        with self._lock:
            table = _text(config.get("目标表"))
            try:
                ratio = float(
                    sample_ratio
                    if sample_ratio is not None
                    else config.get("采样比例", 1)
                )
            except (TypeError, ValueError):
                ratio = 1.0
            method = _text(sample_method or config.get("采样方式")) or "随机采样"
            domain = canonical_domain(context.domain) if context.usable else None
            claims = self.claims_for(table=table, domain=domain) if domain else []
            reason = ""
            status = QUEUED
            blocked = False
            if config.get("status") == "已过期":
                status = NOT_DELIVERED
                blocked = True
                reason = "采样配置已过期，不再接受采样"
            elif not context.usable:
                status = PENDING
                reason = "缺少可信调用上下文，无法确定业务域归属；已保留核对依据"
            elif not claims:
                status = NOT_DELIVERED
                blocked = True
                reason = (
                    f"可信域 {domain} 没有表 {table} 的归属登记；"
                    "表名相同不代表有权读取，已保留核对依据"
                )
            else:
                confirmed = [c for c in claims if c.status == CONFIRMED]
                if not confirmed:
                    status = PENDING
                    reason = f"{domain}域 {table} 的归属尚未确认，等待核对"
                else:
                    approved = [
                        part
                        for claim in confirmed
                        for part in claim.partitions
                        if part.status == APPROVED
                    ]
                    if not approved:
                        status = NOT_DELIVERED
                        blocked = True
                        reason = "归属已确认，但没有已核准的分区可交付"

            if not context.usable:
                # 归属不明时只留核对依据，不创建任何可交付的请求。
                return {
                    "ok": False,
                    "message": "缺少可信调用上下文，无法确定业务域归属；已保留核对依据",
                    "basis": {
                        "manifest_version": self.manifest_version,
                        "table": table,
                        "domain": None,
                        "reason": "缺少可信调用上下文",
                        "trusted": context.evidence(),
                        "claims": [
                            {
                                "claim_id": claim.id,
                                "domain": claim.domain,
                                "ownership_status": claim.status,
                                "partition_count": len(claim.partitions),
                                "projection": "other-domain",
                            }
                            for claim in self.claims_for(table=table)
                        ],
                    },
                    "request": None,
                }
            request = SamplingRequest(
                id=self._next_request_id(),
                config_id=int(config.get("id") or 0) or None,
                config_name=_text(config.get("配置名称")),
                target_table=table,
                sample_ratio=ratio,
                sample_method=method,
                domain=domain,
                principal=context.principal,
                grant=context.grant,
                status=status,
                reason=reason,
                basis=self._basis(
                    table=table,
                    domain=domain,
                    context=context,
                    reason=reason or "采样请求已排队，交付前将重新核对归属清单",
                    claims=claims,
                ),
                manifest_version=self.manifest_version,
                blocked=blocked,
            )
            request.history.append(
                {
                    "at": _now(),
                    "action": "排队采样",
                    "status": status,
                    "actor": context.principal or domain,
                    "manifest_version": self.manifest_version,
                }
            )
            self.requests.append(request)
            if status == QUEUED and config:
                config["采样状态"] = f"排队中（{domain}域）"
            return {
                "ok": status == QUEUED,
                "message": (
                    "采样已排队，交付前会重新核对归属清单"
                    if status == QUEUED
                    else reason
                ),
                "request": request.view(include_rows=False),
            }

    def _resolve_delivery(
        self, request: SamplingRequest
    ) -> tuple[list[dict[str, Any]], list[dict[str, Any]], list[str], list[OwnershipClaim], str]:
        """重新读取归属清单，返回可交付行、血缘、拦截说明。"""
        table = request.target_table
        domain = canonical_domain(request.domain)
        claims = self.claims_for(table=table, domain=domain) if domain else []
        blockers: list[str] = []
        if not domain:
            return [], [], ["缺少可信业务域，归属判定不明确"], claims, PENDING
        if not claims:
            return (
                [],
                [],
                [f"可信域 {domain} 没有表 {table} 的归属登记"],
                claims,
                NOT_DELIVERED,
            )
        confirmed = [c for c in claims if c.status == CONFIRMED]
        if not confirmed:
            claim_states = "、".join(f"{c.domain}:{c.status}" for c in claims)
            return (
                [],
                [],
                [f"{domain}域 {table} 的归属未确认（{claim_states}）"],
                claims,
                PENDING,
            )

        for claim in confirmed:
            for part in claim.partitions:
                if part.status != APPROVED:
                    blockers.append(f"分区 {part.name} 状态为{part.status}，不读取")

        # 按分区与行 ID 稳定排序，比例 1 时取全部；比例不足时按确定性顺序截取。
        candidates: list[tuple[OwnershipClaim, Partition, SampleRow]] = []
        for claim in confirmed:
            for part in claim.partitions:
                if part.status != APPROVED:
                    continue
                for row in sorted(part.rows, key=lambda item: item.row_id):
                    candidates.append((claim, part, row))
        if not candidates:
            return [], [], blockers or ["没有已核准的分区"], claims, NOT_DELIVERED
        total = len(candidates)
        ratio = max(0.0, min(1.0, float(request.sample_ratio)))
        take = total if ratio >= 1.0 else min(total, math.ceil(total * ratio))
        chosen = candidates[:take]

        rows: list[dict[str, Any]] = []
        lineage: list[dict[str, Any]] = []
        for claim, part, row in chosen:
            source = f"iceberg://{claim.domain}/{claim.table}/{part.name}/{row.row_id}"
            rows.append(
                {
                    "row_id": row.row_id,
                    "values": dict(row.values),
                    "domain": claim.domain,
                    "table": claim.table,
                    "partition": part.name,
                    "partition_id": part.id,
                    "claim_id": claim.id,
                    "source": source,
                    "manifest_version": self.manifest_version,
                }
            )
            lineage.append(
                {
                    "request_id": request.id,
                    "domain": claim.domain,
                    "table": claim.table,
                    "claim_id": claim.id,
                    "partition_id": part.id,
                    "partition": part.name,
                    "row_id": row.row_id,
                    "source": source,
                }
            )
        status = DELIVERED if rows else NOT_DELIVERED
        return rows, lineage, blockers, claims, status

    def execute_request(
        self, request_id: int, context: TrustedContext | None = None
    ) -> dict[str, Any]:
        with self._lock:
            request = next((r for r in self.requests if r.id == request_id), None)
            if request is None:
                return {"ok": False, "message": f"采样请求 {request_id} 不存在"}
            if request.status == CANCELLED:
                return {"ok": False, "message": "采样请求已撤销，不再交付"}
            if context is None or not context.usable:
                # 没有可信域就没有读取范围：只留核对依据，不返回任何样本行。
                return {
                    "ok": False,
                    "message": "缺少可信调用上下文，无法确认读取范围，本次不交付",
                    "basis": {
                        "manifest_version": self.manifest_version,
                        "table": request.target_table,
                        "domain": None,
                        "reason": "缺少可信调用上下文",
                        "trusted": (context or TrustedContext(source="missing")).evidence(),
                        "claims": [],
                    },
                    "request": request.view(include_rows=False),
                }
            if canonical_domain(context.domain) != canonical_domain(request.domain):
                return {
                    "ok": False,
                    "message": "可信上下文与排队时的业务域不一致，拒绝交付",
                    "request": request.view(include_rows=False),
                }
            if request.status == DELIVERED:
                return {
                    "ok": True,
                    "message": "采样已交付，返回既有交付记录",
                    "request": request.view(include_rows=True),
                }

            # 排队后配置过期时不再取数：过期是最后一次可信的生命周期变更。
            if request.config_id is not None:
                config = _lookup_config(request.config_id)
                if config is not None and config.get("status") == "已过期":
                    request.status = NOT_DELIVERED
                    request.reason = "采样配置已过期，不再交付"
                    request.history.append(
                        {"at": _now(), "action": "拒绝交付", "reason": request.reason}
                    )
                    return {
                        "ok": False,
                        "message": request.reason,
                        "request": request.view(include_rows=False),
                    }

            rows, lineage, blockers, claims, status = self._resolve_delivery(request)
            request.rows = rows
            request.lineage = lineage
            request.status = status
            request.executed_at = _now()
            request.manifest_version = self.manifest_version
            domain = canonical_domain(request.domain)
            reason = (
                f"已按{domain}域归属清单交付 {len(rows)} 行"
                if status == DELIVERED
                else "；".join(blockers) or "归属清单未给出可交付范围"
            )
            request.reason = reason
            request.basis = self._basis(
                table=request.target_table,
                domain=domain,
                context=TrustedContext(
                    domain=request.domain,
                    principal=request.principal,
                    grant=request.grant,
                    source="request-snapshot",
                ),
                reason=reason,
                claims=claims,
            )
            request.history.append(
                {
                    "at": request.executed_at,
                    "action": "执行采样",
                    "status": status,
                    "row_ids": [row["row_id"] for row in rows],
                    "blockers": blockers,
                    "manifest_version": self.manifest_version,
                }
            )
            if request.config_id is not None:
                config = _lookup_config(request.config_id)
                if config is not None and status == DELIVERED:
                    config["status"] = "已执行"
                    config["pending"] = False
                    config["采样状态"] = (
                        f"已采样{request.target_table}（{domain}域）{len(rows)}行"
                    )
                elif config is not None:
                    config["采样状态"] = f"{status}：{reason}"
            return {
                "ok": status == DELIVERED,
                "message": reason,
                "request": request.view(include_rows=True),
            }

    def cancel_request(self, request_id: int) -> dict[str, Any]:
        request = next((r for r in self.requests if r.id == request_id), None)
        if request is None:
            return {"ok": False, "message": f"采样请求 {request_id} 不存在"}
        request.status = CANCELLED
        request.history.append({"at": _now(), "action": "撤销采样"})
        return {"ok": True, "message": "采样请求已撤销", "request": request.view(include_rows=False)}

    def cancel_requests_for_config(self, config_id: int) -> int:
        count = 0
        for request in self.requests:
            if request.config_id == config_id and request.status == QUEUED:
                self.cancel_request(request.id)
                count += 1
        return count

    def execute_queued(self, context: TrustedContext) -> dict[str, Any]:
        if not context.usable:
            return {"ok": False, "message": "缺少可信调用上下文，未处理任何采样", "results": []}
        domain = canonical_domain(context.domain)
        pending = [
            r for r in self.requests if r.status == QUEUED and r.domain == domain
        ]
        results = [self.execute_request(r.id, context) for r in pending]
        return {
            "ok": all(item.get("ok") for item in results) if results else False,
            "message": f"已处理 {len(results)} 笔排队采样",
            "results": results,
        }

    def list_requests(
        self, context: TrustedContext | None = None
    ) -> list[dict[str, Any]]:
        domain = canonical_domain(context.domain) if context and context.usable else None
        views = []
        for request in self.requests:
            visible = domain is not None and canonical_domain(request.domain) == domain
            views.append(request.view(include_rows=visible))
        return views

    def request_view(
        self, request_id: int, context: TrustedContext | None = None
    ) -> dict[str, Any] | None:
        request = next((r for r in self.requests if r.id == request_id), None)
        if request is None:
            return None
        domain = canonical_domain(context.domain) if context and context.usable else None
        visible = domain is not None and canonical_domain(request.domain) == domain
        view = request.view(include_rows=visible)
        if not visible:
            view["reason"] = (
                "调用方可信域与采样归属不一致，仅返回核对依据，不返回样本行"
                if context and context.usable
                else "未提供可信调用上下文，仅返回核对依据，不返回样本行"
            )
            view["basis"] = (
                self._basis(
                    table=request.target_table,
                    domain=domain,
                    context=context
                    or TrustedContext(source="missing"),
                    reason=view["reason"],
                    claims=self.claims_for(table=request.target_table),
                )
                if domain
                else {
                    "manifest_version": self.manifest_version,
                    "table": request.target_table,
                    "domain": None,
                    "reason": view["reason"],
                    "trusted": (context or TrustedContext(source="missing")).evidence(),
                    "claims": [],
                }
            )
        return view

    # ------------------------------------------------------------------
    # 演示数据
    # ------------------------------------------------------------------
    def reset_demo(self) -> None:
        self.claims = []
        self.requests = []
        self._claim_seq = 0
        self._partition_seq = 0
        self._request_seq = 0
        self.manifest_version = 1

        sales = self.ensure_claim(
            table="订单明细",
            domain="销售",
            owner="销售维护人",
            note="销售域登记的同名表",
        )
        # 含 S1 的分区先处于已核准状态：销售归属确认后即可试采。
        self.add_partition(
            sales,
            name="dt=2026-11-20",
            rows=[{"row_id": "S1", "values": {"id": "S1", "业务域": "销售", "金额": 128}}],
            status=APPROVED,
            note="已核验的销售分区",
        )
        self.add_partition(
            sales,
            name="dt=2026-11-21",
            rows=[{"row_id": "S2", "values": {"id": "S2", "业务域": "销售", "金额": 256}}],
            status=DRAFT,
            note="待核准的销售分区",
        )
        fulfillment = self.ensure_claim(
            table="订单明细",
            domain="履约",
            owner="履约维护人",
            note="履约域登记的同名表",
        )
        self.add_partition(
            fulfillment,
            name="dt=2026-11-20",
            rows=[{"row_id": "F1", "values": {"id": "F1", "业务域": "履约", "状态": "待发"}}],
            status=DRAFT,
            note="履约域分区仍在核对",
        )
        # 演示数据直接预置到当前版本，避免把种子写入当成一次业务变更。
        self.manifest_version = 2


LEDGER = SamplingLedger()


def ledger() -> SamplingLedger:
    return LEDGER


def reset_demo() -> None:
    LEDGER.reset_demo()


def _lookup_config(config_id: int) -> dict[str, Any] | None:
    # 延迟导入，避免 store → services → sampling 的循环依赖。
    from app.store import store

    return store.find("data_sample", config_id)


def register_ownership(
    *,
    table: str,
    domain: str,
    owner: str | None = None,
    note: str = "",
    grant: str | None = None,
    partitions: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    """登记一张表的归属与分区样本；同名表按业务域分开登记。"""
    if not _text(table) or not _text(domain):
        return {"ok": False, "message": "表名与所属业务域均为必填"}
    # 登记内容不能代替权限：新建登记与分区一律从「待核对」起步，
    # 只有该业务域的可信身份才能确认归属、核准分区。
    claim = LEDGER.ensure_claim(
        table=table, domain=domain, owner=owner, note=note, grant=grant
    )
    created: list[dict[str, Any]] = []
    for item in partitions or []:
        name = _text(item.get("name") or item.get("分区"))
        rows = item.get("rows") or item.get("样本行") or []
        part = LEDGER.add_partition(claim, name=name, rows=rows, status=DRAFT, note=_text(item.get("note")))
        created.append(part.view(include_rows=True))
    return {"ok": True, "message": "归属登记已保存，待该业务域可信身份核对", "claim": claim.view(), "partitions": created}
