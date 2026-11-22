"""可信调用上下文：从网关注入的请求头里取身份，不接受请求体自报。

采样涉及跨业务域的读取边界，业务域身份只能来自可信调用链：
- ``X-Trusted-Domain`` / ``X-Trusted-Business-Domain`` / ``X-Domain``
- ``X-Trusted-Principal`` / ``X-Principal``
- ``X-Trusted-Grant``（可选的归属核验凭据）
- ``X-Trusted-Context``（JSON 形式，便于测试/网关一次性注入）

表名、所属业务域等请求体字段只用于描述目标，不能作为权限凭据。
"""
from __future__ import annotations

import json
from dataclasses import dataclass, field
from typing import Any, Mapping

from fastapi import Request

DOMAIN_HEADERS = (
    "x-trusted-domain",
    "x-trusted-business-domain",
    "x-business-domain",
    "x-domain",
)
PRINCIPAL_HEADERS = (
    "x-trusted-principal",
    "x-principal",
    "x-user",
)
GRANT_HEADERS = (
    "x-trusted-grant",
    "x-ownership-grant",
    "x-trusted-ownership-ref",
)
CONTEXT_HEADERS = (
    "x-trusted-context",
    "x-sampling-context",
)

DOMAIN_ALIASES = {
    "sales": "销售",
    "sale": "销售",
    "销售": "销售",
    "销售域": "销售",
    "销售业务域": "销售",
    "fulfillment": "履约",
    "fulfilment": "履约",
    "履约": "履约",
    "履约域": "履约",
    "履约业务域": "履约",
}


def _repair_mojibake(text: str) -> str:
    """HTTP 头按 latin-1 解码；调用方若直接写入 UTF-8 字节，这里还原成中文。"""
    if not text or all(ord(ch) < 128 for ch in text):
        return text
    try:
        repaired = text.encode("latin-1").decode("utf-8")
    except (UnicodeEncodeError, UnicodeDecodeError):
        return text
    return repaired


def canonical_domain(value: str | None) -> str | None:
    """把常见的域别名归一到同一业务域，未知域保持原样。"""
    text = _repair_mojibake(str(value or "").strip())
    if not text:
        return None
    return DOMAIN_ALIASES.get(text.casefold(), text)


@dataclass(frozen=True)
class TrustedContext:
    """由可信调用链注入的身份；为空表示调用方没有可用的归属身份。"""

    domain: str | None = None
    principal: str | None = None
    grant: str | None = None
    source: str = "missing"
    problems: tuple[str, ...] = ()
    raw: Mapping[str, Any] = field(default_factory=dict)

    @property
    def present(self) -> bool:
        return bool(self.domain or self.principal)

    @property
    def usable(self) -> bool:
        return bool(self.domain)

    def evidence(self) -> dict[str, Any]:
        """提供给核对流程的依据：只说明身份来源，不包含任何样本行。"""
        return {
            "trusted": self.usable,
            "domain": self.domain,
            "principal": self.principal,
            "grant": self.grant,
            "source": self.source,
            "problems": list(self.problems),
        }

    def matches_domain(self, domain: str | None) -> bool:
        return bool(self.domain) and canonical_domain(self.domain) == canonical_domain(domain)


def _first_header(headers: Mapping[str, str], names: tuple[str, ...]) -> str | None:
    for name in names:
        value = headers.get(name)
        if value and str(value).strip():
            return str(value).strip()
    return None


def context_from_headers(headers: Mapping[str, str]) -> TrustedContext:
    """解析可信头；JSON 上下文中的字段优先级低于显式头。"""
    problems: list[str] = []
    payload: dict[str, Any] = {}
    raw_context = _first_header(headers, CONTEXT_HEADERS)
    if raw_context:
        try:
            parsed = json.loads(raw_context)
            if isinstance(parsed, dict):
                payload = parsed
            else:
                problems.append("可信上下文不是 JSON 对象")
        except (TypeError, ValueError):
            problems.append("可信上下文 JSON 无法解析")

    domain = _first_header(headers, DOMAIN_HEADERS) or payload.get("domain")
    principal = _first_header(headers, PRINCIPAL_HEADERS) or payload.get("principal")
    grant = _first_header(headers, GRANT_HEADERS) or payload.get("grant")

    source = "headers" if domain or principal else ("context-json" if payload else "missing")
    return TrustedContext(
        domain=canonical_domain(str(domain) if domain is not None else None),
        principal=_repair_mojibake(str(principal).strip()) if principal else None,
        grant=_repair_mojibake(str(grant).strip()) if grant else None,
        source=source,
        problems=tuple(problems),
        raw=payload,
    )


def trusted_context_from_request(request: Request) -> TrustedContext:
    return context_from_headers(request.headers)


def describe_missing_context(context: TrustedContext) -> dict[str, Any]:
    """归属判定不明确时留存的核对依据，不携带任何分区样本。"""
    return {
        "status": "待核对",
        "authorized": False,
        "reason": "缺少可信调用上下文，无法确定业务域归属",
        "basis": context.evidence(),
        "required": ["X-Trusted-Domain", "X-Trusted-Principal"],
    }
