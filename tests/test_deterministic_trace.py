"""Tests for deterministic per-DAG-run trace IDs."""
from unittest.mock import patch

import pytest
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter

import airflow_otel._instrumentation as _impl
from airflow_otel._instrumentation import (
    _DagRunIdGenerator,
    instrument_task,
    shutdown_otel,
)


@pytest.fixture(autouse=True)
def reset_test_exporter():
    yield
    _impl._TEST_EXPORTER = None
    shutdown_otel()


@pytest.fixture
def exporter():
    exp = InMemorySpanExporter()
    _impl._TEST_EXPORTER = exp
    return exp


class TestDagRunIdGenerator:
    def test_same_run_is_deterministic(self):
        assert _DagRunIdGenerator("d", "r1").generate_trace_id() == _DagRunIdGenerator("d", "r1").generate_trace_id()

    def test_different_run_differs(self):
        assert _DagRunIdGenerator("d", "r1").generate_trace_id() != _DagRunIdGenerator("d", "r2").generate_trace_id()

    def test_different_dag_differs(self):
        assert _DagRunIdGenerator("a", "r1").generate_trace_id() != _DagRunIdGenerator("b", "r1").generate_trace_id()

    def test_trace_id_is_valid_128_bit(self):
        tid = _DagRunIdGenerator("d", "r1").generate_trace_id()
        assert 0 < tid < (1 << 128)

    def test_span_ids_are_random_and_nonzero(self):
        g = _DagRunIdGenerator("d", "r1")
        a, b = g.generate_span_id(), g.generate_span_id()
        assert a != 0 and b != 0 and a != b


class TestRunScopedGrouping:
    @staticmethod
    def _run_root_trace_id(fn, ctx):
        exp = InMemorySpanExporter()
        _impl._TEST_EXPORTER = exp
        fn(**ctx)
        roots = [s for s in exp.get_finished_spans() if s.parent is None]
        assert len(roots) == 1, f"expected one root span, got {len(roots)}"
        return roots[0].context.trace_id

    def test_orphan_tasks_in_same_run_share_trace(self, mock_airflow_context):
        mock_airflow_context["ti"].task.upstream_task_ids = []
        with patch("airflow_otel._context.get_current_context", return_value=mock_airflow_context):
            @instrument_task
            def task_one(**kwargs):
                pass

            @instrument_task
            def task_two(**kwargs):
                pass

            tid_one = self._run_root_trace_id(task_one, mock_airflow_context)
            tid_two = self._run_root_trace_id(task_two, mock_airflow_context)
        assert tid_one == tid_two

    def test_different_runs_get_different_traces(self, mock_airflow_context):
        mock_airflow_context["ti"].task.upstream_task_ids = []
        with patch("airflow_otel._context.get_current_context", return_value=mock_airflow_context):
            @instrument_task
            def my_task(**kwargs):
                pass

            tid_one = self._run_root_trace_id(my_task, mock_airflow_context)
            mock_airflow_context["dag_run"].run_id = "manual__2024-02-02T00:00:00+00:00"
            tid_two = self._run_root_trace_id(my_task, mock_airflow_context)
        assert tid_one != tid_two

    def test_upstream_carrier_trace_id_overrides_generator(self, exporter, mock_airflow_context):
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
        span = next(s for s in exporter.get_finished_spans() if s.name == "my_dag.my_task")
        assert format(span.context.trace_id, "032x") == trace_id
        assert span.parent is not None
        assert format(span.parent.span_id, "016x") == span_id
