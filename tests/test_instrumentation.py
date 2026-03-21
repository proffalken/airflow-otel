"""
Tests for airflow_otel instrumentation library.

All span assertions use InMemorySpanExporter so no collector is needed.
_TEST_EXPORTER is the seam: set it before calling an instrumented task;
SimpleSpanProcessor is used automatically, so spans are captured synchronously.
"""
import logging
import pytest
from unittest.mock import ANY, patch
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter
from opentelemetry.trace import SpanKind, StatusCode

import airflow_otel._instrumentation as _impl
from airflow_otel._context import extract_airflow_context
from airflow_otel._instrumentation import (
    get_tracer,
    instrument_task,
    instrument_task_context,
    setup_otel,
    shutdown_otel,
)
from airflow_otel._propagation import OTEL_XCOM_KEY


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _no_parentless_client_spans(spans):
    bad = [s for s in spans if s.kind in (SpanKind.CLIENT, SpanKind.PRODUCER) and not s.parent]
    assert not bad, f"Parentless CLIENT/PRODUCER spans: {[s.name for s in bad]}"


def _error_spans_have_messages(spans):
    bad = [s for s in spans if s.status.status_code == StatusCode.ERROR and not s.status.description]
    assert not bad, f"ERROR spans without status message: {[s.name for s in bad]}"


@pytest.fixture(autouse=True)
def reset_test_exporter():
    """Ensure _TEST_EXPORTER is cleared and OTel is shut down between tests."""
    yield
    _impl._TEST_EXPORTER = None
    shutdown_otel()


@pytest.fixture
def exporter():
    exp = InMemorySpanExporter()
    _impl._TEST_EXPORTER = exp
    return exp


# ---------------------------------------------------------------------------
# Context extraction
# ---------------------------------------------------------------------------

class TestExtractAirflowContext:
    def test_extracts_from_dict(self, mock_airflow_context):
        attrs = extract_airflow_context(mock_airflow_context)
        assert attrs["dag_id"] == "my_dag"
        assert attrs["task_id"] == "my_task"
        assert attrs["run_id"] == "scheduled__2024-01-15T00:00:00+00:00"
        assert attrs["try_number"] == 1
        assert attrs["logical_date"] == "2024-01-15T00:00:00+00:00"

    def test_falls_back_to_get_current_context(self, mock_airflow_context):
        with patch("airflow_otel._context.get_current_context", return_value=mock_airflow_context):
            attrs = extract_airflow_context({})
        assert attrs["dag_id"] == "my_dag"
        assert attrs["task_id"] == "my_task"

    def test_graceful_on_missing_keys(self):
        attrs = extract_airflow_context({})
        assert "dag_id" not in attrs

    def test_falls_back_to_kwargs_when_airflow_unavailable(self, mock_airflow_context):
        with patch("airflow_otel._context.get_current_context", side_effect=Exception("no context")):
            attrs = extract_airflow_context(mock_airflow_context)
        assert attrs["dag_id"] == "my_dag"


# ---------------------------------------------------------------------------
# Resource attributes
# ---------------------------------------------------------------------------

class TestResourceAttributes:
    def test_service_name_is_task_id(self, exporter):
        setup_otel("my_dag", "my_task", "run-123", 1)
        with get_tracer().start_as_current_span("probe"):
            pass
        spans = exporter.get_finished_spans()
        assert spans
        assert spans[0].resource.attributes["service.name"] == "my_task"
        assert spans[0].resource.attributes["service.namespace"] == "my_dag"

    def test_service_instance_id_is_uuid(self, exporter):
        import re
        setup_otel("my_dag", "my_task", "run-123", 1)
        with get_tracer().start_as_current_span("probe"):
            pass
        instance_id = exporter.get_finished_spans()[0].resource.attributes.get("service.instance.id", "")
        assert re.match(
            r"^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$",
            instance_id,
        ), f"service.instance.id is not a UUID v4: {instance_id!r}"

    def test_run_id_not_in_resource(self, exporter):
        setup_otel("my_dag", "my_task", "scheduled__2024-01-15", 2)
        with get_tracer().start_as_current_span("probe"):
            pass
        resource_attrs = dict(exporter.get_finished_spans()[0].resource.attributes)
        for v in resource_attrs.values():
            assert "scheduled__2024-01-15" not in str(v), \
                f"run_id found in resource attribute (cardinality risk): {resource_attrs}"

    def test_try_number_not_in_resource(self, exporter):
        setup_otel("my_dag", "my_task", "run-999", 3)
        with get_tracer().start_as_current_span("probe"):
            pass
        resource_attrs = dict(exporter.get_finished_spans()[0].resource.attributes)
        # try_number should NOT appear as a standalone resource attribute
        assert "airflow.try_number" not in resource_attrs


# ---------------------------------------------------------------------------
# Root span
# ---------------------------------------------------------------------------

class TestRootSpan:
    def test_root_span_is_consumer(self, exporter, mock_airflow_context):
        with patch("airflow_otel._context.get_current_context", return_value=mock_airflow_context):
            @instrument_task
            def my_task(**kwargs):
                pass
            my_task(**mock_airflow_context)

        root_spans = [s for s in exporter.get_finished_spans() if s.parent is None]
        assert len(root_spans) == 1
        assert root_spans[0].kind == SpanKind.CONSUMER

    def test_root_span_name(self, exporter, mock_airflow_context):
        with patch("airflow_otel._context.get_current_context", return_value=mock_airflow_context):
            @instrument_task
            def my_task(**kwargs):
                pass
            my_task(**mock_airflow_context)

        root = next(s for s in exporter.get_finished_spans() if s.parent is None)
        assert root.name == "my_dag.my_task"

    def test_root_span_carries_airflow_attributes(self, exporter, mock_airflow_context):
        with patch("airflow_otel._context.get_current_context", return_value=mock_airflow_context):
            @instrument_task
            def my_task(**kwargs):
                pass
            my_task(**mock_airflow_context)

        root = next(s for s in exporter.get_finished_spans() if s.parent is None)
        assert root.attributes["airflow.dag_id"] == "my_dag"
        assert root.attributes["airflow.task_id"] == "my_task"
        assert root.attributes["airflow.run_id"] == "scheduled__2024-01-15T00:00:00+00:00"
        assert root.attributes["airflow.try_number"] == 1

    def test_no_parentless_client_spans(self, exporter, mock_airflow_context):
        with patch("airflow_otel._context.get_current_context", return_value=mock_airflow_context):
            @instrument_task
            def my_task(**kwargs):
                with get_tracer().start_as_current_span("child", kind=SpanKind.CLIENT):
                    pass
            my_task(**mock_airflow_context)

        _no_parentless_client_spans(exporter.get_finished_spans())


# ---------------------------------------------------------------------------
# Error handling
# ---------------------------------------------------------------------------

class TestErrorHandling:
    def test_exception_sets_error_status(self, exporter, mock_airflow_context):
        with patch("airflow_otel._context.get_current_context", return_value=mock_airflow_context):
            @instrument_task
            def failing_task(**kwargs):
                raise ValueError("something went wrong")
            with pytest.raises(ValueError):
                failing_task(**mock_airflow_context)

        root = next(s for s in exporter.get_finished_spans() if s.parent is None)
        assert root.status.status_code == StatusCode.ERROR

    def test_error_span_has_message(self, exporter, mock_airflow_context):
        with patch("airflow_otel._context.get_current_context", return_value=mock_airflow_context):
            @instrument_task
            def failing_task(**kwargs):
                raise RuntimeError("database unreachable")
            with pytest.raises(RuntimeError):
                failing_task(**mock_airflow_context)

        _error_spans_have_messages(exporter.get_finished_spans())
        root = next(s for s in exporter.get_finished_spans() if s.parent is None)
        assert "RuntimeError" in root.status.description
        assert "database unreachable" in root.status.description

    def test_exception_is_reraised(self, exporter, mock_airflow_context):
        with patch("airflow_otel._context.get_current_context", return_value=mock_airflow_context):
            @instrument_task
            def failing_task(**kwargs):
                raise ValueError("airflow needs to see this")
            with pytest.raises(ValueError, match="airflow needs to see this"):
                failing_task(**mock_airflow_context)

    def test_success_sets_ok_status(self, exporter, mock_airflow_context):
        with patch("airflow_otel._context.get_current_context", return_value=mock_airflow_context):
            @instrument_task
            def ok_task(**kwargs):
                return "done"
            result = ok_task(**mock_airflow_context)

        assert result == "done"
        root = next(s for s in exporter.get_finished_spans() if s.parent is None)
        assert root.status.status_code == StatusCode.OK


# ---------------------------------------------------------------------------
# instrument_task_context (traditional PythonOperator)
# ---------------------------------------------------------------------------

class TestInstrumentTaskContext:
    def test_context_manager_creates_consumer_root_span(self, exporter, mock_airflow_context):
        with patch("airflow_otel._context.get_current_context", return_value=mock_airflow_context):
            def my_callable(**context):
                with instrument_task_context(context):
                    pass
            my_callable(**mock_airflow_context)

        root_spans = [s for s in exporter.get_finished_spans() if s.parent is None]
        assert len(root_spans) == 1
        assert root_spans[0].kind == SpanKind.CONSUMER

    def test_context_manager_propagates_exception(self, exporter, mock_airflow_context):
        with patch("airflow_otel._context.get_current_context", return_value=mock_airflow_context):
            def my_callable(**context):
                with instrument_task_context(context):
                    raise KeyError("missing key")
            with pytest.raises(KeyError):
                my_callable(**mock_airflow_context)

        root = next(s for s in exporter.get_finished_spans() if s.parent is None)
        assert root.status.status_code == StatusCode.ERROR

    def test_context_manager_yields_span(self, exporter, mock_airflow_context):
        with patch("airflow_otel._context.get_current_context", return_value=mock_airflow_context):
            def my_callable(**context):
                with instrument_task_context(context) as span:
                    span.set_attribute("records.processed", 42)
            my_callable(**mock_airflow_context)

        root = next(s for s in exporter.get_finished_spans() if s.parent is None)
        assert root.attributes["records.processed"] == 42


# ---------------------------------------------------------------------------
# Cross-task trace propagation via XCom
# ---------------------------------------------------------------------------

class TestCrossTaskPropagation:
    def test_no_upstream_creates_root_span(self, exporter, mock_airflow_context):
        """Tasks with no upstream tasks create independent root spans."""
        mock_airflow_context["ti"].task.upstream_task_ids = []

        with patch("airflow_otel._context.get_current_context", return_value=mock_airflow_context):
            @instrument_task
            def my_task(**kwargs):
                pass
            my_task(**mock_airflow_context)

        task_span = next(s for s in exporter.get_finished_spans() if s.name == "my_dag.my_task")
        assert task_span.parent is None

    def test_upstream_context_used_as_parent(self, exporter, mock_airflow_context):
        """Span is created as child of the upstream task's trace context from XCom."""
        trace_id = "4bf92f3577b34da6a3ce929d0e0e4736"
        span_id = "00f067aa0ba902b7"
        carrier = {"traceparent": f"00-{trace_id}-{span_id}-01"}

        ti = mock_airflow_context["ti"]
        ti.task.upstream_task_ids = ["upstream_task"]
        ti.xcom_pull.return_value = carrier

        with patch("airflow_otel._context.get_current_context", return_value=mock_airflow_context):
            @instrument_task
            def my_task(**kwargs):
                pass
            my_task(**mock_airflow_context)

        task_span = next(s for s in exporter.get_finished_spans() if s.name == "my_dag.my_task")
        assert task_span.parent is not None
        assert format(task_span.parent.trace_id, "032x") == trace_id
        assert format(task_span.parent.span_id, "016x") == span_id

    def test_current_context_pushed_to_xcom(self, exporter, mock_airflow_context):
        """After the task span is created, the W3C context is pushed to XCom."""
        ti = mock_airflow_context["ti"]
        ti.task.upstream_task_ids = []

        with patch("airflow_otel._context.get_current_context", return_value=mock_airflow_context):
            @instrument_task
            def my_task(**kwargs):
                pass
            my_task(**mock_airflow_context)

        ti.xcom_push.assert_called_once_with(key=OTEL_XCOM_KEY, value=ANY)
        pushed_carrier = ti.xcom_push.call_args[1]["value"]
        assert "traceparent" in pushed_carrier

    def test_fan_in_uses_first_available_upstream(self, exporter, mock_airflow_context):
        """For fan-in (multiple upstreams), uses first task that has XCom context."""
        trace_id = "4bf92f3577b34da6a3ce929d0e0e4736"
        span_id = "00f067aa0ba902b7"
        carrier = {"traceparent": f"00-{trace_id}-{span_id}-01"}

        ti = mock_airflow_context["ti"]
        ti.task.upstream_task_ids = ["task_a", "task_b"]

        def xcom_pull_side_effect(task_ids, key):
            return carrier if task_ids == "task_a" else None

        ti.xcom_pull.side_effect = xcom_pull_side_effect

        with patch("airflow_otel._context.get_current_context", return_value=mock_airflow_context):
            @instrument_task
            def my_task(**kwargs):
                pass
            my_task(**mock_airflow_context)

        task_span = next(s for s in exporter.get_finished_spans() if s.name == "my_dag.my_task")
        assert task_span.parent is not None
        assert format(task_span.parent.trace_id, "032x") == trace_id

    def test_context_manager_upstream_context_used_as_parent(self, exporter, mock_airflow_context):
        """instrument_task_context also links to upstream via XCom."""
        trace_id = "4bf92f3577b34da6a3ce929d0e0e4736"
        span_id = "00f067aa0ba902b7"
        carrier = {"traceparent": f"00-{trace_id}-{span_id}-01"}

        ti = mock_airflow_context["ti"]
        ti.task.upstream_task_ids = ["upstream_task"]
        ti.xcom_pull.return_value = carrier

        with patch("airflow_otel._context.get_current_context", return_value=mock_airflow_context):
            def my_callable(**context):
                with instrument_task_context(context):
                    pass
            my_callable(**mock_airflow_context)

        task_span = next(s for s in exporter.get_finished_spans() if s.name == "my_dag.my_task")
        assert task_span.parent is not None
        assert format(task_span.parent.trace_id, "032x") == trace_id
