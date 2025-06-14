# OpenLineage

I want to get openlineage data into AWS DataZone from Python.

But I'm using Marquez as an OL backend first, to get it working locally,
since trying to log OL data to AWS out of the gate would confound the learning curve of OL.

I followed the marquez tutorial [here](https://openlineage.io/docs/guides/airflow-quickstart/#get-marquez).

## Step 1 - Start marquez locally

To start up a marquez server, do

```bash
cd ./marquez
bash ./docker/up.sh --db-port 12345 --api-port 9000 --no-volumes --seed
```

Then go to http://localhost:3000 to see the UI

![](../openlineage-playground/docs/marquez-ui.png)

## Step 2 - Log some sample lineage events (no extension)

```bash
OPENLINEAGE__TRANSPORT__TYPE=http \
OPENLINEAGE__TRANSPORT__URL=http://localhost:9000 \
OPENLINEAGE__TRANSPORT__ENDPOINT=/api/v1/lineage \
OPENLINEAGE__TRANSPORT__COMPRESSION=gzip \
    uv run ./examples/log_lineage.py
```

Note, no API key is needed to send data to marquez, hence not specifying an auth type in these environment variables ^^^.

## Step 3 - Run a metaflow flow

```bash
OPENLINEAGE__TRANSPORT__TYPE=http \
OPENLINEAGE__TRANSPORT__URL=http://localhost:9000 \
OPENLINEAGE__TRANSPORT__ENDPOINT=/api/v1/lineage \
OPENLINEAGE__TRANSPORT__COMPRESSION=gzip \
    uv run ./examples/lineage_flow.py run
```