"""业务路由汇总：所有模块共用一套资源路由，由领域服务完成分派。"""
from __future__ import annotations

from app.routers import resource, pipeline, sampling

# 固定前缀的路由必须排在通用资源路由前：/api/sampling/** 有三级路径，
# 否则会被 /api/{module}/{entry_id} 抢先匹配后按整数校验失败。
ROUTERS = [sampling, pipeline, resource]
