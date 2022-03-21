"""
Prefect integration with DataJoint - providing a more complete workflow management experience
Requires `prefect` - see https://github.com/prefecthq/prefect
"""


import inspect
import datajoint as dj
import logging
from prefect import Parameter, Flow, task
from prefect import Client
from prefect.run_configs import UniversalRun
from prefect.storage import Local
from prefect.executors import LocalExecutor
from prefect.engine.state import Cancelled
from prefect.triggers import all_successful, all_failed, all_finished, any_failed, any_successful, manual_only

from .query import get_flow_runs, get_flow_id


log = logging.getLogger(__name__)


_prefect_trigger_mapper = {
    'all_successful': all_successful,
    'all_failed': all_failed,
    'all_finished': all_finished,
    'any_failed': any_failed,
    'any_successful': any_successful,
    'manual_only': manual_only,
}

_populate_settings = {
    "reserve_jobs": True,
    "suppress_errors": True,
}


class DataJointFlow:
    """
    A decorator class to convert DataJoint pipeline (or portion of the pipeline) into prefect Flow
    """

    def __init__(self, project_name, flow_name, flow_labels=[], storage=None, run_config=None, executor=None):
        self.project_name = project_name
        self.flow_name = flow_name
        self.flow_labels = flow_labels + [flow_name]

        self.processes = {}
        self.tasks = {}
        self.task_count = -1
        self._terminal_process = None

        self.storage = storage or Local(add_default_labels=False)
        self.run_config = run_config or UniversalRun()
        self.executor = executor or LocalExecutor()

        self.prefect_client = Client()
        self._flow, self._trigger_flow = None, None

    def __call__(self, process, trigger='all_successful'):
        self.task_count += 1
        self.processes[self.task_count] = process
        if isinstance(process, dj.user_tables.TableMeta):
            @task(name=process.__name__, trigger=_prefect_trigger_mapper[trigger])
            def flow_task(restrictions):
                job_errors = process.populate(*restrictions, **_populate_settings)
                if len(job_errors):
                    error_msg = f'Populate Task failed - {len(job_errors)} job errors.\n'
                    error_msg += '\n'.join([j for _, j in job_errors])
                    # delete error jobs from job-table
                    vmod = dj.create_virtual_module(process.database, process.database)
                    (vmod.schema.jobs & [{'key_hash': dj.hash.key_hash(k)} for k, _ in job_errors]).delete()
                    raise dj.errors.DataJointError(error_msg)

            self.tasks[self.task_count] = flow_task
            self._terminal_process = self.task_count
        elif inspect.isfunction(process) or inspect.ismethod(process):
            @task(name=process.__name__, trigger=_prefect_trigger_mapper[trigger])
            def flow_task(restrictions):
                process()
            self.tasks[self.task_count] = flow_task
        else:
            raise NotImplemented(
                f"Unable to handle processing step of type {type(process)}"
            )

    @property
    def flow(self):
        if self._flow is None:
            keys = Parameter("keys", default=[])
            with Flow(self.flow_name, storage=self.storage,
                      run_config=self.run_config, executor=self.executor) as f:
                flow_output = {}
                for i, flowtask in self.tasks.items():
                    flow_output[i] = flowtask(keys)
                    if i > 0:
                        f.set_dependencies(task=flow_output[i], upstream_tasks=[flow_output[i-1]])

            fid = f.register(self.project_name, labels=self.flow_labels)
            self._flow = (fid, f)
        return self._flow[-1]

    @property
    def trigger_flow(self):
        """
        This flow accomplishes two goals
        1. schedule new flow runs based on the `key_source` of the parent table
        2. cancel staled scheduled runs
        """
        if self._trigger_flow is None:
            assert self.flow is not None

            if not isinstance(self.processes[0], dj.user_tables.TableMeta):
                return None

            @task(name=self.flow_name + '_trigger')
            def flow_trigger():
                flow_id = get_flow_id(self.flow_name, self.project_name)
                keys_todo = self.processes[0].key_source - self.processes[self._terminal_process]
                keys_todo = keys_todo.fetch('KEY')

                log.info(f'Creating {len(keys_todo)} flow run(s) - Flow ID: {flow_id}')
                for key in keys_todo:
                    self.prefect_client.create_flow_run(
                        flow_id=flow_id,
                        parameters={"keys": [key]},
                        run_name=str(key),
                        idempotency_key=dj.hash.key_hash(key)
                    )

                keys_todo = [str(k) for k in keys_todo]

                scheduled_runs = get_flow_runs(self.flow_name, self.project_name, 'Failed')
                runs_to_cancel = [r.id for r in scheduled_runs if r.name not in keys_todo]

                log.info(f'Cancelling {len(runs_to_cancel)} flow run(s)')
                cancelled_state = Cancelled('Invalid scheduled flow run - table(s) may be populated elsewhere')
                for r_id in runs_to_cancel:
                    self.prefect_client.set_flow_run_state(r_id, cancelled_state, version=None)

            with Flow(self.flow_name + '_trigger', storage=self.storage,
                      run_config=self.run_config, executor=self.executor) as f:
                flow_trigger()

            fid = f.register(self.project_name, labels=self.flow_labels)
            self._trigger_flow = (fid, f)
        return self._trigger_flow[-1]

    def register(self):
        self.flow
        self.trigger_flow
