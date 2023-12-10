from dagster import ScheduleDefinition, build_schedule_from_partitioned_job
from ..jobs import revenue_update_job

revenue_update_schedule = build_schedule_from_partitioned_job(
    revenue_update_job
)