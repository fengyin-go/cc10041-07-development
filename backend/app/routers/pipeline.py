"""流批一体管道API：流式处理、批量编排、检查点恢复、Schema兼容、快照管理。"""
from __future__ import annotations

from typing import Any

from fastapi import APIRouter, HTTPException, Query, Request

from app.engine import StreamPipeline, BatchPipelineEngine, PipelineMonitorEngine, DataQualityEngine
from app.trusted import trusted_context_from_request

router = APIRouter(prefix="/api/pipeline", tags=["数据处理管道"])


@router.post("/stream/run")
def run_stream_pipeline(batch_id: str | None = Query(default=None)) -> dict[str, Any]:
    """执行一次流处理管线：消费→清洗→窗口→聚合→迟到→写入→检查点。"""
    pipeline = StreamPipeline()
    return pipeline.run(batch_id=batch_id)


@router.get("/stream/checkpoint/latest")
def get_latest_checkpoint() -> dict[str, Any]:
    """获取最新检查点状态。"""
    pipeline = StreamPipeline()
    return pipeline.recover_from_checkpoint()


@router.post("/batch/dag/run")
def run_batch_dag(dag_name: str | None = Query(default=None)) -> dict[str, Any]:
    """执行批量DAG编排管道。"""
    engine = BatchPipelineEngine()
    return engine.run_dag(dag_name=dag_name)


@router.post("/batch/backfill")
def run_backfill(time_range: str | None = Query(default=None)) -> dict[str, Any]:
    """执行历史回填任务。"""
    engine = BatchPipelineEngine()
    return engine.run_backfill(time_range=time_range)


@router.get("/monitor/metrics")
def collect_pipeline_metrics() -> dict[str, Any]:
    """采集管道运行时指标。"""
    engine = PipelineMonitorEngine()
    return engine.collect_metrics()


@router.post("/quality/schema/check")
def check_schema_compatibility(
    schema_name: str = Query(...),
    new_fields: str = Query(...),
) -> dict[str, Any]:
    """检查Schema兼容性。"""
    engine = DataQualityEngine()
    fields = [f.strip() for f in new_fields.split(",") if f.strip()]
    return engine.check_schema_compatibility(schema_name=schema_name, new_fields=fields)


@router.post("/quality/iceberg/snapshot")
def create_iceberg_snapshot(table_name: str = Query(...)) -> dict[str, Any]:
    """为Iceberg表创建快照。"""
    engine = DataQualityEngine()
    result = engine.create_snapshot(table_name)
    if not result.get("snapshot_created"):
        raise HTTPException(status_code=404, detail=result.get("reason", "未知错误"))
    return result


@router.post("/quality/sample")
def execute_sampling(
    request: Request,
    target_table: str = Query(...),
    sample_ratio: float = Query(default=0.1, ge=0.0, le=1.0),
) -> dict[str, Any]:
    """执行数据采样；表名进入归属清单后必须带该业务域的可信身份。"""
    engine = DataQualityEngine()
    context = trusted_context_from_request(request)
    return engine.execute_sampling(
        target_table=target_table, sample_ratio=sample_ratio, context=context
    )


@router.get("/quality/health")
def check_health(
    records_processed: int = Query(default=100),
    records_failed: int = Query(default=0),
) -> dict[str, Any]:
    """检查管道健康状态。"""
    engine = PipelineMonitorEngine()
    return engine.check_pipeline_health(records_processed, records_failed)