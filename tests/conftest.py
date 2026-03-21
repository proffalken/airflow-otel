import pytest
from unittest.mock import MagicMock
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry.sdk.resources import Resource


@pytest.fixture
def in_memory_exporter():
    exporter = InMemorySpanExporter()
    yield exporter
    exporter.clear()


@pytest.fixture
def tracer_provider(in_memory_exporter):
    resource = Resource.create({"service.name": "test", "service.namespace": "test-dag"})
    provider = TracerProvider(resource=resource)
    provider.add_span_processor(SimpleSpanProcessor(in_memory_exporter))
    yield provider
    provider.shutdown()


@pytest.fixture
def mock_airflow_context():
    """Minimal Airflow context dict matching what get_current_context() returns."""
    dag = MagicMock()
    dag.dag_id = "my_dag"

    task = MagicMock()
    task.task_id = "my_task"

    dag_run = MagicMock()
    dag_run.run_id = "scheduled__2024-01-15T00:00:00+00:00"
    dag_run.logical_date = MagicMock()
    dag_run.logical_date.isoformat.return_value = "2024-01-15T00:00:00+00:00"

    ti = MagicMock()
    ti.try_number = 1
    ti.task_id = "my_task"
    ti.dag_id = "my_dag"

    return {
        "dag": dag,
        "task": task,
        "dag_run": dag_run,
        "task_instance": ti,
        "ti": ti,
    }
