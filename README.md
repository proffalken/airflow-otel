# airflow-otel

OpenTelemetry instrumentation for Apache Airflow tasks.

Wraps Airflow task execution with traces, metrics, and logs exported via OTLP. Designed to work with any OpenTelemetry-compatible backend (Jaeger, Tempo, Dash0, Honeycomb, etc.) — point it at your collector and go.

## Features

- Creates a `CONSUMER` root span per task execution named `{dag_id}.{task_id}`
- **Cross-task trace linking** via W3C Trace Context propagation through XCom — tasks in a DAG run appear as a single connected trace in Dash0, Grafana App O11y, Jaeger, etc.
- Sets `ERROR` status and emits a correlated log record on exception
- Supports both the **TaskFlow API** (decorator) and **traditional PythonOperator** (context manager)
- Cardinality-safe: high-cardinality values (`run_id`, `try_number`, `logical_date`) go on span/log attributes, never on resource attributes
- `get_tracer()` / `get_meter()` give you access for custom child spans and metrics inside tasks

## Installation

```bash
pip install airflow-otel
```

Or with [uv](https://github.com/astral-sh/uv):

```bash
uv add airflow-otel
```

## Configuration

Configuration is via environment variables — no code changes needed to switch environments.

| Variable | Default | Description |
|---|---|---|
| `OTEL_EXPORTER_OTLP_ENDPOINT` | `http://localhost:4318` | OTLP HTTP endpoint |
| `OTEL_EXPORTER_OTLP_HEADERS` | _(none)_ | Comma-separated `key=value` headers (e.g. for auth tokens) |
| `OTEL_RESOURCE_ATTRIBUTES` | _(none)_ | Extra resource attributes as comma-separated `key=value` pairs |
| `AIRFLOW_ENV` / `ENV` | `production` | Sets `deployment.environment.name` on the resource |

## Usage

### TaskFlow API — `@instrument_task` decorator

Apply `@instrument_task` below `@task`. The decorator handles OTel setup and teardown automatically.

```python
from airflow.decorators import dag, task
from airflow_otel import instrument_task, get_tracer, get_meter
from datetime import datetime

@dag(schedule=None, start_date=datetime(2024, 1, 1))
def my_pipeline():

    @task
    @instrument_task
    def process_records():
        tracer = get_tracer()
        meter = get_meter()

        records_counter = meter.create_counter(
            "records.processed",
            description="Number of records processed",
        )

        with tracer.start_as_current_span("fetch") as span:
            records = fetch_from_source()
            span.set_attribute("records.fetched", len(records))

        with tracer.start_as_current_span("transform"):
            transformed = [transform(r) for r in records]

        records_counter.add(len(transformed))
        return len(transformed)

    process_records()

dag_instance = my_pipeline()
```

### Traditional PythonOperator — `instrument_task_context` context manager

```python
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow_otel import instrument_task_context, get_tracer
from datetime import datetime

def process_records(**context):
    with instrument_task_context(context) as span:
        tracer = get_tracer()

        with tracer.start_as_current_span("fetch") as child:
            records = fetch_from_source()
            child.set_attribute("records.fetched", len(records))

        # Add attributes to the root span
        span.set_attribute("records.processed", len(records))

with DAG("my_dag", start_date=datetime(2024, 1, 1), schedule=None) as dag:
    PythonOperator(
        task_id="process_records",
        python_callable=process_records,
    )
```

## What gets emitted

### Resource attributes

These are stable for the lifetime of the task process and safe for metric cardinality:

| Attribute | Value |
|---|---|
| `service.name` | `task_id` |
| `service.namespace` | `dag_id` |
| `service.instance.id` | Random UUID v4 per process |
| `deployment.environment.name` | `AIRFLOW_ENV` / `ENV` env var, or `production` |

### Root span attributes

High-cardinality values are placed on the span (not the resource) to avoid unbounded metric time series:

| Attribute | Value |
|---|---|
| `airflow.dag_id` | DAG identifier |
| `airflow.task_id` | Task identifier |
| `airflow.run_id` | Run identifier |
| `airflow.try_number` | Attempt number |
| `airflow.logical_date` | Logical execution date (if available) |

## Cross-task trace linking

Each task automatically propagates the W3C `traceparent` header through Airflow's XCom under the key `__otel_trace_context__`.

- **Before** starting the root span: the library pulls context from each upstream task's XCom and uses it as the parent, linking this task's span into the same trace.
- **After** the root span starts: the library injects the current context into XCom so downstream tasks can pick it up.

The result is that all tasks in a DAG run appear as a single connected trace in your observability tool's service graph, with each task as a child span of its upstream.

For fan-in patterns (multiple upstream tasks), the first upstream task that has a stored context is used as the parent.

## Development

```bash
# Install with dev dependencies
uv sync --extra dev

# Run tests
uv run pytest

# Run tests with coverage
uv run pytest --cov=airflow_otel
```

Tests use an in-memory span exporter — no running collector required.
