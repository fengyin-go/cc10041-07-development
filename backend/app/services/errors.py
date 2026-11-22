"""领域规则异常：路由层只负责转成可读结果，不在路由里做业务判断。"""
from __future__ import annotations


class BusinessRuleError(Exception):
    """可预期的业务拒绝，例如归属与可信调用域不一致。"""
