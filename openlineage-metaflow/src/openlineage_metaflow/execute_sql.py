from datetime import datetime
from metaflow import current
from openlineage.client import OpenLineageClient
from openlineage.client.event_v2 import (
    Dataset,
    InputDataset,
    OutputDataset,
    RunEvent,
    RunState,
    Run,
    Job,
)
from openlineage.client.facet import SqlJobFacet
from openlineage_sql import parse
import sqlglot


def _create_openlineage_client() -> OpenLineageClient:
    """Initialize OpenLineage client from environment variables."""
    return OpenLineageClient()


def execute_sql(query: str, dialect: str = "snowflake") -> None:
    """
    Mock SQL execution that emits OpenLineage events with input/output datasets.

    Args:
        query: SQL query string to parse and extract lineage from
        dialect: SQL dialect to use for parsing (default: snowflake)
    """
    try:
        # Parse SQL to extract input/output datasets
        statements = [stmt.sql() for stmt in sqlglot.parse(query, read=dialect)]

        # Use openlineage_sql to parse the query for lineage
        parsed_result = parse(sql=statements, dialect=dialect)

        # Get current step context
        step_ctx = getattr(current, "_open_lineage_ctx", None)
        if not step_ctx:
            print("Warning: No OpenLineage context found")
            return

        # Get current step name
        step_name = current.step_name
        if step_name not in step_ctx["steps"]:
            print(f"Warning: Step {step_name} not found in OpenLineage context")
            return

        step_info = step_ctx["steps"][step_name]
        job: Job = step_info["job"]
        run: Run = step_info["run"]

        # Create input and output datasets from parsed SQL
        inputs = []
        outputs = []

        if parsed_result:
            for input_table in parsed_result.in_tables:
                table_namespace = input_table.database or ""
                table_namespace += (
                    f".{input_table.schema}" if input_table.schema else ""
                )
                table_namespace += f".{input_table.name}" if input_table.name else ""
                input_dataset = InputDataset(
                    namespace=table_namespace,
                    name=input_table.name,
                )

                inputs.append(input_dataset)

            for output_table in parsed_result.out_tables:
                table_namespace = output_table.database or ""
                table_namespace += (
                    f".{output_table.schema}" if output_table.schema else ""
                )
                table_namespace += f".{output_table.name}" if output_table.name else ""

                output_dataset = OutputDataset(
                    namespace=table_namespace,
                    name=output_table.name,
                )
                outputs.append(output_dataset)

        # Update run with SQL facets
        run.facets["sql"] = SqlJobFacet(query=query)
        
        # Create and emit OTHER event
        client = _create_openlineage_client()

        event = RunEvent(
            eventType=RunState.OTHER,
            eventTime=datetime.utcnow().isoformat(),
            run=run,
            job=job,
            producer="metaflow-openlineage-extension",
            inputs=inputs,
            outputs=outputs,
        )

        client.emit(event)

    except Exception as e:
        print(f"Error executing SQL lineage: {e}")
        raise
