# /// script
# requires-python = ">=3.12"
# dependencies = [
#     "openlineage-python",
# ]
# ///

from datetime import datetime, timezone
from openlineage.client import OpenLineageClient
from openlineage.client.transport.http import HttpConfig, HttpCompression, HttpTransport
from openlineage.client.event_v2 import (
    RunEvent,
    Run,
    Job,
    InputDataset,
    OutputDataset,
    Dataset,
    RunState,
)
from openlineage.client.facet_v2 import (
    nominal_time_run,
    source_code_location_job,
    schema_dataset,
)
from openlineage.client.uuid import generate_new_uuid

from datetime import timedelta

# === CONFIG ===
PRODUCER = "https://github.com/openlineage-user"
NAMESPACE = "example-namespace"
REPO_URL = "https://github.com/your-org/example-pipeline.py"

# # instantiating the client with HTTP transport
# http_config = HttpConfig(
#     url="http://localhost:9000",
#     endpoint="api/v1/lineage",
#     timeout=5,
#     verify=False,
#     compression=HttpCompression.GZIP,
# )

# client = OpenLineageClient(transport=HttpTransport(http_config))

client = OpenLineageClient()

now = datetime.now(timezone.utc)

# Define job
job = Job(
    namespace=NAMESPACE,
    name="example-job-name",
    facets={
        "sourceCodeLocation": source_code_location_job.SourceCodeLocationJobFacet(
            type="git", 
            url=REPO_URL
        )
    }
)

# Define datasets
input_dataset = InputDataset(
    namespace=NAMESPACE,
    name="input-dataset-name",
    facets={
        "schema": schema_dataset.SchemaDatasetFacet(fields=[
            schema_dataset.SchemaDatasetFacetFields("field1", "STRING"),
            schema_dataset.SchemaDatasetFacetFields("field2", "INT")
        ])
    }
)

output_dataset = OutputDataset(
    namespace=NAMESPACE,
    name="output-dataset-name",
    facets={
        "schema": schema_dataset.SchemaDatasetFacet(fields=[
            schema_dataset.SchemaDatasetFacetFields("field1", "STRING"),
            schema_dataset.SchemaDatasetFacetFields("field2", "INT")
        ])
    }
)

run_id = generate_new_uuid()
start = RunEvent(
    eventType=RunState.START,
    eventTime=now.isoformat(),
    run=Run(
        runId=str(run_id),
        facets={
            "nominalTime": nominal_time_run.NominalTimeRunFacet(
                nominalStartTime=now.isoformat()
            )
        }
    ),
    job=job,
    producer=PRODUCER,
    inputs=[input_dataset],
    outputs=[output_dataset]
)
complete = RunEvent(
    eventType=RunState.COMPLETE,
    eventTime=(now + timedelta(minutes=5)).isoformat(),
    run=Run(
        runId=str(run_id),
        facets={
            "nominalTime": nominal_time_run.NominalTimeRunFacet(
                nominalStartTime=now.isoformat()
            )
        }
    ),
    job=job,
    # producer=PRODUCER,
    # inputs=[input_dataset],
    # outputs=[output_dataset]
)

client.emit(start)
client.emit(complete)
