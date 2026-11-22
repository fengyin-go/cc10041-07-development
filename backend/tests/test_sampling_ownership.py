"""采样归属校验测试：同名表按业务域隔离、撤回的分区不读取、归属不明只留依据。

覆盖需求里的完整链路：
  销售/履约登记同名表 → 只确认销售归属 → 两域各自看范围 →
  工作台执行采样 → 排队后撤回含 S1 分区并核准含 S2 分区 →
  交付只含 S2，S1 与 F1 都读不到，且每行可追到真实来源。
"""
from __future__ import annotations

import sys
from pathlib import Path

import pytest

backend_root = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(backend_root))

from app.sampling import APPROVED, CONFIRMED, REVOKED, ledger, reset_demo
from app.seed import SEED_ROWS
from app.store import store
from app.trusted import TrustedContext, canonical_domain, context_from_headers


SALES = TrustedContext(domain="销售", principal="销售维护人", source="headers")
FULFILLMENT = TrustedContext(domain="履约", principal="履约维护人", source="headers")
ANONYMOUS = TrustedContext(source="missing")


@pytest.fixture(autouse=True)
def fresh_state():
    store._tables = {name: [dict(row) for row in rows] for name, rows in SEED_ROWS.items()}
    reset_demo()
    yield


def claim_for(domain: str):
    matches = [c for c in ledger().claims if c.domain == domain and c.table == "订单明细"]
    assert matches, f"{domain}域应登记了订单明细"
    return matches[0]


def claims_of(table: str = "订单明细"):
    return ledger().claims_for(table=table)


def request_by_id(request_id: int):
    return next(r for r in ledger().requests if r.id == request_id)


# ---------------------------------------------------------------------------
# 归属清单按业务域隔离
# ---------------------------------------------------------------------------

def test_same_table_name_is_registered_per_domain():
    sales, fulfillment = claims_of()
    assert {sales.domain, fulfillment.domain} == {"销售", "履约"}
    assert sales.id != fulfillment.id
    assert sales.partitions and fulfillment.partitions
    # 两个域各有自己的分区，不共享也不互相覆盖
    assert {p.row_ids[0] for p in sales.partitions} == {"S1", "S2"}
    assert {p.row_ids[0] for p in fulfillment.partitions} == {"F1"}


def test_scope_only_exposes_own_domain():
    sales_scope = ledger().table_scope("订单明细", SALES)
    assert sales_scope["authorized"] is True
    own = next(d for d in sales_scope["domains"] if d["domain"] == "销售")
    other = next(d for d in sales_scope["domains"] if d["domain"] == "履约")
    assert own["visible"] is True and own["partitions"]
    assert other["visible"] is False and other["partitions"] == []
    assert sales_scope["same_name_domains"] == ["履约"]

    fulfillment_scope = ledger().table_scope("订单明细", FULFILLMENT)
    own_f = next(d for d in fulfillment_scope["domains"] if d["domain"] == "履约")
    assert own_f["visible"] is True
    sales_view = next(d for d in fulfillment_scope["domains"] if d["domain"] == "销售")
    assert sales_view["visible"] is False and sales_view["partitions"] == []


def test_legacy_config_has_no_domain_so_table_name_alone_is_not_a_credential():
    config = store.find("data_sample", 1)
    scope = ledger().legacy_config_scope(config, SALES)
    assert scope["has_stored_domain"] is False
    # 旧配置只存表名，但范围仍按可信域解析
    assert scope["target_table"] == "订单明细"
    assert scope["authorized"] is True
    anonymous = ledger().legacy_config_scope(config, ANONYMOUS)
    assert anonymous["authorized"] is False
    assert all(not d["visible"] for d in anonymous["domains"])


def test_anonymous_context_keeps_basis_without_partitions():
    scope = ledger().table_scope("订单明细", ANONYMOUS)
    assert scope["authorized"] is False
    assert all(d["partitions"] == [] for d in scope["domains"])
    assert "表名本身不能作为读取凭据" in scope["reason"] or scope["reason"]
    assert scope["basis"]["trusted"]["trusted"] is False
    # 核对依据里也不能泄露样本行
    assert "S1" not in str(scope)


# ---------------------------------------------------------------------------
# 归属核对：一个域确认、另一个域仍待核对
# ---------------------------------------------------------------------------

def test_confirm_one_domain_leaves_other_pending():
    sales, fulfillment = claims_of()
    result = ledger().confirm_claim(sales.id, SALES)
    assert result["ok"] is True
    assert sales.status == CONFIRMED
    assert fulfillment.status != CONFIRMED

    # 履约域仍不能读到任何分区
    scope = ledger().table_scope("订单明细", FULFILLMENT)
    fulfillment_view = next(d for d in scope["domains"] if d["domain"] == "履约")
    assert fulfillment_view["ownership_status"] != CONFIRMED
    queued = ledger().queue_request(config=store.find("data_sample", 1), context=FULFILLMENT)
    request = request_by_id(queued["request"]["request_id"])
    delivered = ledger().execute_request(request.id, FULFILLMENT)
    assert delivered["ok"] is False
    assert all(row["row_id"] != "F1" for row in delivered["request"]["rows"])


def test_cannot_confirm_with_other_domain_identity():
    sales, _ = claims_of()
    result = ledger().confirm_claim(sales.id, FULFILLMENT)
    assert result["ok"] is False
    assert sales.status != CONFIRMED
    assert "basis" in result


def test_cannot_approve_partition_before_claim_confirmed():
    fulfillment = claim_for("履约")
    part = fulfillment.partitions[0]
    result = ledger().approve_partition(part.id, FULFILLMENT)
    assert result["ok"] is False
    assert part.status != APPROVED


# ---------------------------------------------------------------------------
# 完整场景：排队后撤回 S1、核准 S2
# ---------------------------------------------------------------------------

def test_requested_scenario_delivers_only_approved_sales_partition():
    sales, fulfillment = claims_of()

    # 1. 只确认销售归属
    assert ledger().confirm_claim(sales.id, SALES)["ok"] is True

    # 2. 旧配置看两域范围（销售/履约身份分别提供）
    sales_scope = ledger().legacy_config_scope(store.find("data_sample", 1), SALES)
    fulfillment_scope = ledger().legacy_config_scope(store.find("data_sample", 1), FULFILLMENT)
    assert [d["domain"] for d in sales_scope["domains"]] == ["销售", "履约"]
    assert [d["domain"] for d in fulfillment_scope["domains"]] == ["销售", "履约"]
    assert next(d for d in fulfillment_scope["domains"] if d["domain"] == "销售")["visible"] is False

    # 3. 工作台执行采样动作：先试采一次
    service = __import__("app.services", fromlist=["service_for"]).service_for("data_sample")
    entry, message = service.run_action(1, "执行采样", SALES)
    assert entry is not None and "排队" in message
    first = ledger().execute_queued(SALES)["results"][0]
    assert first["ok"] is True
    assert {row["row_id"] for row in first["request"]["rows"]} == {"S1"}

    # 4. 排队一笔销售采样
    result = ledger().queue_request(
        config=store.find("data_sample", 1), context=SALES, sample_ratio=1
    )
    assert result["ok"] is True
    request_id = result["request"]["request_id"]
    request = request_by_id(request_id)
    assert request.rows == []  # 排队阶段不读取任何行

    # 5. 执行前撤回含 S1 的分区，核准含 S2 的销售分区
    s1_part = next(p for p in sales.partitions if "S1" in p.row_ids)
    s2_part = next(p for p in sales.partitions if "S2" in p.row_ids)
    assert ledger().revoke_partition(s1_part.id, SALES)["ok"] is True
    assert ledger().approve_partition(s2_part.id, SALES)["ok"] is True
    assert s1_part.status == REVOKED and s2_part.status == APPROVED

    # 6. 执行交付
    delivered = ledger().execute_request(request_id, SALES)
    assert delivered["ok"] is True
    rows = delivered["request"]["rows"]
    assert [row["row_id"] for row in rows] == ["S2"]
    assert all(row["domain"] == "销售" for row in rows)
    assert all("F1" not in row["row_id"] for row in rows)
    assert all(row["source"] for row in rows)
    assert rows[0]["source"] == "iceberg://销售/订单明细/dt=2026-11-21/S2"

    # 7. 按请求查交付，销售可读、履约只拿依据
    sales_view = ledger().request_view(request_id, SALES)
    assert sales_view["delivered"] is True
    assert sales_view["row_ids"] == ["S2"]
    fulfillment_view = ledger().request_view(request_id, FULFILLMENT)
    assert fulfillment_view["rows"] == []
    assert fulfillment_view["row_count"] is None
    assert fulfillment_view["lineage"] == []
    anonymous_view = ledger().request_view(request_id, ANONYMOUS)
    assert anonymous_view["rows"] == []
    assert anonymous_view["row_count"] is None


def test_revoked_partition_is_never_read_even_if_previously_approved():
    sales, _ = claims_of()
    ledger().confirm_claim(sales.id, SALES)
    s1_part = next(p for p in sales.partitions if "S1" in p.row_ids)
    assert s1_part.status == APPROVED

    queued = ledger().queue_request(config=store.find("data_sample", 1), context=SALES)
    ledger().revoke_partition(s1_part.id, SALES)

    # 即使 S2 没有核准，也不能回退去读已撤回的 S1
    delivered = ledger().execute_request(queued["request"]["request_id"], SALES)
    assert delivered["request"]["rows"] == []
    assert delivered["ok"] is False
    assert "S1" not in str(delivered["request"]["rows"])


def test_fulfillment_identity_cannot_execute_sales_request():
    sales, _ = claims_of()
    ledger().confirm_claim(sales.id, SALES)
    queued = ledger().queue_request(config=store.find("data_sample", 1), context=SALES)
    result = ledger().execute_request(queued["request"]["request_id"], FULFILLMENT)
    assert result["ok"] is False
    assert request_by_id(queued["request"]["request_id"]).rows == []


def test_queue_without_trusted_context_keeps_basis_and_no_rows():
    result = ledger().queue_request(config=store.find("data_sample", 1), context=ANONYMOUS)
    assert result["ok"] is False
    assert result["request"] is None
    assert result["basis"]["trusted"]["trusted"] is False
    assert ledger().requests == []
    # 匿名调用不能借任何已有请求读到行
    assert "S1" not in str(result) and "F1" not in str(result)


def test_approval_requires_matching_domain_identity():
    sales, _ = claims_of()
    ledger().confirm_claim(sales.id, SALES)
    s2_part = next(p for p in sales.partitions if "S2" in p.row_ids)
    result = ledger().approve_partition(s2_part.id, FULFILLMENT)
    assert result["ok"] is False
    assert s2_part.status != APPROVED


def test_cancelled_request_never_delivers():
    sales, _ = claims_of()
    ledger().confirm_claim(sales.id, SALES)
    queued = ledger().queue_request(config=store.find("data_sample", 1), context=SALES)
    request_id = queued["request"]["request_id"]
    ledger().cancel_request(request_id)
    result = ledger().execute_request(request_id, SALES)
    assert result["ok"] is False
    assert request_by_id(request_id).rows == []


# ---------------------------------------------------------------------------
# 可信上下文解析 & Iceberg 归属登记
# ---------------------------------------------------------------------------

@pytest.mark.parametrize(
    "headers, expected",
    [
        ({"x-trusted-domain": "销售", "x-trusted-principal": "张三"}, "销售"),
        ({"x-trusted-domain": "sales", "x-trusted-principal": "张三"}, "销售"),
        ({"x-trusted-domain": "履约", "x-trusted-principal": "李四"}, "履约"),
        ({"x-trusted-domain": "fulfillment", "x-trusted-principal": "李四"}, "履约"),
    ],
)
def test_trusted_context_headers(headers, expected):
    context = context_from_headers(headers)
    assert context.usable is True
    assert context.domain == expected


def test_missing_trusted_context_is_not_usable():
    context = context_from_headers({})
    assert context.usable is False
    assert context.present is False


def test_iceberg_registration_creates_ownership_claim():
    from app.services.iceberg_table import IcebergTableService

    service = IcebergTableService()
    entry, missing = service.create_entry(
        {"表名": "退款单", "所属业务域": "销售", "存储格式": "Parquet"}, SALES
    )
    assert entry is not None and missing == []
    claims = ledger().claims_for(table="退款单")
    assert [c.domain for c in claims] == ["销售"]
    assert claims[0].status == "待核对"


def test_iceberg_registration_rejects_cross_domain_claim():
    from app.services.errors import BusinessRuleError
    from app.services.iceberg_table import IcebergTableService

    service = IcebergTableService()
    with pytest.raises(BusinessRuleError):
        service.create_entry(
            {"表名": "订单明细", "所属业务域": "履约", "存储格式": "Parquet"}, SALES
        )


def test_iceberg_registration_rejects_duplicate_in_same_domain():
    from app.services.errors import BusinessRuleError
    from app.services.iceberg_table import IcebergTableService

    service = IcebergTableService()
    with pytest.raises(BusinessRuleError):
        service.create_entry(
            {"表名": "订单明细", "所属业务域": "销售", "存储格式": "Parquet"}, SALES
        )


def test_same_name_in_two_domains_is_allowed_at_registration():
    from app.services.iceberg_table import IcebergTableService

    service = IcebergTableService()
    sales_entry, _ = service.create_entry(
        {"表名": "共享报表", "所属业务域": "销售", "存储格式": "Parquet"}, SALES
    )
    fulfillment_entry, _ = service.create_entry(
        {"表名": "共享报表", "所属业务域": "履约", "存储格式": "Parquet"}, FULFILLMENT
    )
    assert sales_entry is not None and fulfillment_entry is not None
    assert {c.domain for c in ledger().claims_for(table="共享报表")} == {"销售", "履约"}


def test_domain_alias_normalization():
    assert canonical_domain("sales") == "销售"
    assert canonical_domain("Fulfillment") == "履约"
    assert canonical_domain("销售") == "销售"
    assert canonical_domain("") is None


# ---------------------------------------------------------------------------
# 采样工作台接口：可信头 → 路由 → 归属清单
# ---------------------------------------------------------------------------

def _call(method: str, path: str, *, headers=None, body=None) -> tuple[int, dict]:
    import asyncio
    import json
    from urllib.parse import urlsplit

    from app.main import app

    parts = urlsplit(path)
    raw_headers = [(b"content-type", b"application/json")]
    for key, value in (headers or {}).items():
        raw_headers.append((key.lower().encode(), value.encode()))
    payload = json.dumps(body).encode() if body is not None else b""
    messages = []

    async def receive():
        return {"type": "http.request", "body": payload, "more_body": False}

    async def send(message):
        messages.append(message)

    scope = {
        "type": "http",
        "asgi": {"version": "3.0", "spec_version": "2.3"},
        "http_version": "1.1",
        "method": method,
        "path": parts.path,
        "raw_path": parts.path.encode(),
        "query_string": parts.query.encode(),
        "root_path": "",
        "headers": raw_headers,
        "client": ("127.0.0.1", 5555),
        "server": ("127.0.0.1", 8016),
        "scheme": "http",
    }
    asyncio.run(app(scope, receive, send))
    status = next(m["status"] for m in messages if m["type"] == "http.response.start")
    raw = b"".join(
        m.get("body", b"") for m in messages if m["type"] == "http.response.body"
    )
    return status, json.loads(raw) if raw else {}


SALES_HEADERS = {"X-Trusted-Domain": "销售", "X-Trusted-Principal": "销售维护人"}
FULFILLMENT_HEADERS = {"X-Trusted-Domain": "履约", "X-Trusted-Principal": "履约维护人"}


def test_http_workbench_full_flow():
    status, manifest = _call("GET", "/api/sampling/manifest", headers=SALES_HEADERS)
    assert status == 200
    sales_claim = next(c for c in manifest["claims"] if c["domain"] == "销售")

    status, _ = _call(
        "POST", f"/api/sampling/claims/{sales_claim['claim_id']}/confirm", headers=SALES_HEADERS
    )
    assert status == 200

    # 旧配置看两域范围
    status, scope = _call(
        "GET", "/api/sampling/legacy-scope?config_id=1", headers=FULFILLMENT_HEADERS
    )
    assert status == 200
    visible = {d["domain"]: d["visible"] for d in scope["domains"]}
    assert visible == {"销售": False, "履约": True}

    # 工作台排队
    status, queued = _call(
        "POST", "/api/sampling/requests", headers=SALES_HEADERS,
        body={"config_id": 1, "sample_ratio": 1},
    )
    assert status == 200 and queued["ok"] is True
    request_id = queued["request"]["request_id"]

    # 撤回 S1、核准 S2
    partitions = {
        p["name"]: p["partition_id"] for p in sales_claim["partitions"]
    }
    _call("POST", f"/api/sampling/partitions/{partitions['dt=2026-11-20']}/revoke",
          headers=SALES_HEADERS)
    _call("POST", f"/api/sampling/partitions/{partitions['dt=2026-11-21']}/approve",
          headers=SALES_HEADERS)

    status, executed = _call(
        "POST", f"/api/sampling/requests/{request_id}/execute", headers=SALES_HEADERS
    )
    assert status == 200 and executed["ok"] is True

    status, delivery = _call(
        "GET", f"/api/sampling/requests/{request_id}/delivery", headers=SALES_HEADERS
    )
    assert delivery["delivered"] is True
    assert delivery["row_ids"] == ["S2"]
    assert delivery["can_trace_sources"] is True
    assert delivery["rows"][0]["source"].startswith("iceberg://销售/订单明细/")

    # 履约视角拿不到行
    status, other = _call(
        "GET", f"/api/sampling/requests/{request_id}/delivery", headers=FULFILLMENT_HEADERS
    )
    assert other["rows"] == [] and other["row_ids"] == []

    # 无可信上下文时执行被拒
    _call("POST", "/api/sampling/requests", headers=SALES_HEADERS,
          body={"config_id": 1, "sample_ratio": 1})
    anonymous_id = ledger().requests[-1].id
    status, denied = _call("POST", f"/api/sampling/requests/{anonymous_id}/execute")
    assert status == 401


def test_http_queue_without_identity_creates_no_request():
    status, result = _call(
        "POST", "/api/sampling/requests", body={"config_id": 1, "sample_ratio": 1}
    )
    assert result["ok"] is False
    assert result["request"] is None
    assert ledger().requests == []

