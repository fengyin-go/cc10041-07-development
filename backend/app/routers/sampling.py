"""采样工作台接口：归属核对、范围查看、排队、执行、交付查询。

所有者身份只从可信调用上下文（网关头）读取；请求体里的表名与业务域只描述目标，
不能当作读取凭据。旧配置只存表名时，按可信域解析两域范围。
"""
from __future__ import annotations

from typing import Any

from fastapi import APIRouter, Depends, HTTPException, Query, Request
from pydantic import BaseModel, ConfigDict, Field

from app import sampling
from app.trusted import (
    TrustedContext,
    describe_missing_context,
    trusted_context_from_request,
)

router = APIRouter(prefix="/api/sampling", tags=["采样工作台"])


def trusted(request: Request) -> TrustedContext:
    return trusted_context_from_request(request)


class ClaimPayload(BaseModel):
    model_config = ConfigDict(extra="allow")
    table: str
    domain: str
    owner: str | None = None
    note: str = ""
    grant: str | None = None
    partitions: list[dict[str, Any]] = Field(default_factory=list)


class PartitionPayload(BaseModel):
    model_config = ConfigDict(extra="allow")
    note: str = ""


class QueuePayload(BaseModel):
    model_config = ConfigDict(extra="allow")
    config_id: int | None = None
    target_table: str | None = None
    sample_ratio: float | None = None
    sample_method: str | None = None


@router.get("/manifest")
def read_manifest(context: TrustedContext = Depends(trusted)) -> dict[str, Any]:
    """按可信域读取归属清单；他域只暴露归属状态，不返回样本行。"""
    claims = []
    for claim in sampling.ledger().claims:
        visible = context.usable and context.matches_domain(claim.domain)
        claim_view = claim.view(include_rows=visible)
        if not visible:
            claim_view["owner"] = None
            claim_view["partitions"] = []
            claim_view["projection"] = "other-domain"
        claims.append(claim_view)
    return {
        "manifest_version": sampling.ledger().manifest_version,
        "trusted": context.evidence() if context.present else describe_missing_context(context),
        "authorized": context.usable,
        "claims": claims,
    }


@router.post("/claims")
def register_claim(
    payload: ClaimPayload,
    context: TrustedContext = Depends(trusted),
) -> dict[str, Any]:
    """登记归属与分区样本；登记内容不能代替该域的可信身份。"""
    if context.usable and not context.matches_domain(payload.domain):
        raise HTTPException(
            status_code=403,
            detail={
                "message": "登记的业务域与可信调用域不一致",
                "trusted": context.evidence(),
            },
        )
    result = sampling.register_ownership(
        table=payload.table,
        domain=payload.domain,
        owner=payload.owner,
        note=payload.note,
        grant=payload.grant,
        partitions=payload.partitions,
    )
    if not result.get("ok"):
        raise HTTPException(status_code=422, detail=result.get("message"))
    return result


@router.post("/claims/{claim_id}/partitions")
def register_partition(
    claim_id: int,
    payload: dict[str, Any],
    context: TrustedContext = Depends(trusted),
) -> dict[str, Any]:
    """向已有归属登记追加分区样本；需由该业务域的可信身份发起。"""
    if not context.usable:
        raise HTTPException(status_code=401, detail=describe_missing_context(context))
    result = sampling.ledger().add_partition_by_id(
        claim_id,
        name=str(payload.get("name") or ""),
        rows=payload.get("rows") or payload.get("样本行") or [],
        status=str(payload.get("status") or sampling.DRAFT),
        note=str(payload.get("note") or ""),
        context=context,
    )
    if not result.get("ok"):
        raise HTTPException(status_code=403, detail=result)
    return result


@router.post("/claims/{claim_id}/confirm")
def confirm_claim(claim_id: int, context: TrustedContext = Depends(trusted)) -> dict[str, Any]:
    if not context.usable:
        raise HTTPException(status_code=401, detail=describe_missing_context(context))
    result = sampling.ledger().confirm_claim(claim_id, context)
    if not result.get("ok"):
        raise HTTPException(status_code=403, detail=result)
    return result


@router.post("/claims/{claim_id}/reject")
def reject_claim(claim_id: int, context: TrustedContext = Depends(trusted)) -> dict[str, Any]:
    if not context.usable:
        raise HTTPException(status_code=401, detail=describe_missing_context(context))
    result = sampling.ledger().reject_claim(claim_id, context)
    if not result.get("ok"):
        raise HTTPException(status_code=403, detail=result)
    return result


@router.post("/partitions/{partition_id}/approve")
def approve_partition(partition_id: int, context: TrustedContext = Depends(trusted)) -> dict[str, Any]:
    if not context.usable:
        raise HTTPException(status_code=401, detail=describe_missing_context(context))
    result = sampling.ledger().approve_partition(partition_id, context)
    if not result.get("ok"):
        raise HTTPException(status_code=403, detail=result)
    return result


@router.post("/partitions/{partition_id}/revoke")
def revoke_partition(
    partition_id: int,
    payload: PartitionPayload | None = None,
    context: TrustedContext = Depends(trusted),
) -> dict[str, Any]:
    if not context.usable:
        raise HTTPException(status_code=401, detail=describe_missing_context(context))
    result = sampling.ledger().revoke_partition(
        partition_id, context, note=(payload.note if payload else "")
    )
    if not result.get("ok"):
        raise HTTPException(status_code=403, detail=result)
    return result


@router.get("/scope")
def table_scope(
    table: str = Query(...),
    context: TrustedContext = Depends(trusted),
) -> dict[str, Any]:
    return sampling.ledger().table_scope(table, context)


@router.get("/legacy-scope")
def legacy_config_scope(
    config_id: int = Query(...),
    context: TrustedContext = Depends(trusted),
) -> dict[str, Any]:
    from app.store import store

    config = store.find("data_sample", config_id)
    if config is None:
        raise HTTPException(status_code=404, detail=f"采样配置 {config_id} 不存在")
    return sampling.ledger().legacy_config_scope(config, context)


@router.post("/requests")
def queue_request(
    payload: QueuePayload,
    context: TrustedContext = Depends(trusted),
) -> dict[str, Any]:
    """排队一笔采样；只记录可信域身份与清单快照，不读取任何分区行。"""
    from app.store import store

    config: dict[str, Any]
    if payload.config_id is not None:
        found = store.find("data_sample", payload.config_id)
        if found is None:
            raise HTTPException(status_code=404, detail=f"采样配置 {payload.config_id} 不存在")
        config = found
    elif payload.target_table:
        config = {
            "配置名称": f"{payload.target_table} 临时采样",
            "目标表": payload.target_table,
            "采样比例": payload.sample_ratio if payload.sample_ratio is not None else 1,
            "采样方式": payload.sample_method or "随机采样",
        }
    else:
        raise HTTPException(status_code=422, detail="需要 config_id 或 target_table")
    result = sampling.ledger().queue_request(
        config=config,
        context=context,
        sample_ratio=payload.sample_ratio,
        sample_method=payload.sample_method,
    )
    result["status_code"] = 200 if result.get("ok") else 202
    return result


@router.post("/requests/{request_id}/execute")
def execute_request(
    request_id: int,
    context: TrustedContext = Depends(trusted),
) -> dict[str, Any]:
    """执行排队中的采样：重新读取归属清单，只交付已确认且已核准的分区。"""
    if not context.usable:
        raise HTTPException(status_code=401, detail=describe_missing_context(context))
    return sampling.ledger().execute_request(request_id, context)


@router.post("/requests/{request_id}/cancel")
def cancel_request(
    request_id: int, context: TrustedContext = Depends(trusted)
) -> dict[str, Any]:
    if not context.usable:
        raise HTTPException(status_code=401, detail=describe_missing_context(context))
    view = sampling.ledger().request_view(request_id, context)
    if view is None:
        raise HTTPException(status_code=404, detail=f"采样请求 {request_id} 不存在")
    if not context.matches_domain(view.get("domain")):
        raise HTTPException(status_code=403, detail="可信上下文与采样归属的业务域不一致")
    return sampling.ledger().cancel_request(request_id)


@router.get("/requests")
def list_requests(context: TrustedContext = Depends(trusted)) -> dict[str, Any]:
    return {
        "trusted": context.evidence() if context.present else describe_missing_context(context),
        "items": sampling.ledger().list_requests(context),
    }


@router.get("/requests/{request_id}")
def get_request(request_id: int, context: TrustedContext = Depends(trusted)) -> dict[str, Any]:
    view = sampling.ledger().request_view(request_id, context)
    if view is None:
        raise HTTPException(status_code=404, detail=f"采样请求 {request_id} 不存在")
    return view


@router.get("/requests/{request_id}/delivery")
def request_delivery(
    request_id: int,
    context: TrustedContext = Depends(trusted),
) -> dict[str, Any]:
    """按请求查是否交付、交付了哪几行，以及每行可追溯到真实来源。"""
    view = sampling.ledger().request_view(request_id, context)
    if view is None:
        raise HTTPException(status_code=404, detail=f"采样请求 {request_id} 不存在")
    rows = view.get("rows") or []
    return {
        "request_id": request_id,
        "domain": view.get("domain"),
        "target_table": view.get("target_table"),
        "delivered": bool(view.get("delivered")),
        "status": view.get("status"),
        "visible": view.get("rows_visible"),
        "row_count": view.get("row_count"),
        "row_ids": view.get("row_ids") or [],
        "rows": rows,
        "lineage": view.get("lineage") or [],
        "can_trace_sources": all(row.get("source") for row in rows)
        if view.get("rows_visible")
        else False,
        "reason": view.get("reason"),
        "basis": view.get("basis"),
        "manifest_version": view.get("manifest_version"),
        "trusted": context.evidence() if context.present else describe_missing_context(context),
    }


@router.post("/execute-queued")
def execute_queued(context: TrustedContext = Depends(trusted)) -> dict[str, Any]:
    if not context.usable:
        raise HTTPException(status_code=401, detail=describe_missing_context(context))
    return sampling.ledger().execute_queued(context)
