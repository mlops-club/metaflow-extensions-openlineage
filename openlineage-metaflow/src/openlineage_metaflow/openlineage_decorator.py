"""
Note: if Airflow uses OpenLineage, and Airflow dags trigger
Metaflow flows, then we will need Airflow to pass
it's Job/Run ID to the Metaflow flow so we can set it as the root.

Note: if we use the 'resume' command, then start gets skipped.
But with this code, the flow-level job is created in the start
step... so how do we account for this?
"""
import functools
from datetime import datetime
from typing import Callable
from metaflow import current
from openlineage.client import OpenLineageClient
from openlineage.client.run import RunEvent, RunState, Run, Job
from openlineage.client.uuid import generate_new_uuid
from openlineage.client.facet import ParentRunFacet


SCHEDULER_NAMESPACE = "metaflow"


def _create_openlineage_client() -> OpenLineageClient:
    """Initialize OpenLineage client from environment variables."""
    return OpenLineageClient()


def _create_step_job_and_run(flow_name: str, step_name: str, step_run_id: str, flow_run_id: str) -> tuple[Job, Run]:
    """Create OpenLineage Job and Run objects for step-level events."""
    job_name = f"{flow_name}.{step_name}"
    namespace = flow_name
    
    parent_run_facet = ParentRunFacet(
        run={"runId": flow_run_id},
        job={"namespace": SCHEDULER_NAMESPACE, "name": flow_name}
    )
    
    job = Job(namespace=namespace, name=job_name)
    run = Run(runId=step_run_id, facets={"parent": parent_run_facet})
    
    return job, run


def _create_flow_job_and_run(flow_name: str, run_id: str) -> tuple[Job, Run]:
    """Create OpenLineage Job and Run objects for flow-level events."""
    job = Job(namespace=SCHEDULER_NAMESPACE, name=flow_name)
    run = Run(runId=run_id)
    return job, run


def _emit_run_event(client: OpenLineageClient, event_type: RunState, job: Job, run: Run) -> None:
    """Emit an OpenLineage run event."""
    event = RunEvent(
        eventType=event_type,
        eventTime=datetime.utcnow().isoformat(),
        run=run,
        job=job,
        producer="metaflow-openlineage-extension"
    )
    client.emit(event)


def openlineage(func: Callable) -> Callable:
    """
    Decorator for Metaflow steps that emits OpenLineage events.
    
    For 'start' step: Emits START event for both the whole flow and the start step
    For 'end' step: Emits COMPLETE event for both the whole flow and the end step
    For any step failure: Emits FAIL event for the whole flow
    Job name format: <FlowName> for flow-level, <FlowName>.<step_name> for step-level
    Namespace: metaflow
    """
    
    @functools.wraps(func)
    def wrapper(self, *args, **kwargs):
        # Initialize OpenLineage context if it doesn't exist
        if not hasattr(self, '_open_lineage_ctx'):
            self._open_lineage_ctx = {
                'flow_job': None,
                'flow_run': None,
                'steps': {}  # step_name -> {'job': Job, 'run': Run}
            }
        
        client = _create_openlineage_client()
        
        flow_name = self.__class__.__name__
        step_name = func.__name__
        
        flow_run_id = str(generate_new_uuid())
        step_run_id = str(generate_new_uuid())

        step_job, step_run = _create_step_job_and_run(flow_name, step_name, str(step_run_id), str(flow_run_id))
        
        # Store step job and run in context
        self._open_lineage_ctx['steps'][step_name] = {
            'job': step_job,
            'run': step_run
        }
        
        # Emit START events for 'start' step (both flow and step level)
        if step_name == "start":
            flow_job, flow_run = _create_flow_job_and_run(flow_name, flow_run_id)
            # Store flow job and run in context
            self._open_lineage_ctx['flow_job'] = flow_job
            self._open_lineage_ctx['flow_run'] = flow_run
            _emit_run_event(client, RunState.START, flow_job, flow_run)
        
        _emit_run_event(client, RunState.START, step_job, step_run)
        
        try:
            # Execute the original step function
            result = func(self, *args, **kwargs)
            
            _emit_run_event(client, RunState.COMPLETE, step_job, step_run)

            # Emit COMPLETE events for 'end' step (both flow and step level)
            if step_name == "end":
                flow_job = self._open_lineage_ctx['flow_job']
                flow_run = self._open_lineage_ctx['flow_run']
                if flow_job and flow_run:
                    _emit_run_event(client, RunState.COMPLETE, flow_job, flow_run)
            
            return result
            
        except Exception as e:
            # Emit FAIL event for the whole flow on any step failure
            flow_job = self._open_lineage_ctx.get('flow_job')
            flow_run = self._open_lineage_ctx.get('flow_run')
            if not flow_job or not flow_run:
                flow_job, flow_run = _create_flow_job_and_run(flow_name, flow_run_id)
            # Emit FAIL event
            try:
                _emit_run_event(client, RunState.FAIL, flow_job, flow_run)
                _emit_run_event(client, RunState.FAIL, step_job, step_run)
            except Exception as emit_exception:
                print(f"Error emitting fail event: {emit_exception}")
            raise
    
    return wrapper



