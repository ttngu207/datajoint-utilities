"""
Prefect integration with DataJoint - providing a more complete workflow management experience
Requires `prefect` - see https://github.com/prefecthq/prefect
"""

import inspect
import datajoint as dj
from prefect import task, flow
from prefect.deployments import Deployment, run_deployment
from prefect.orion.schemas.schedules import IntervalSchedule

from prefect.filesystems import LocalFileSystem

logger = dj.logger

_populate_settings = {
    "reserve_jobs": True,
    "suppress_errors": True,
}


class DataJointFlow:
    """
    A decorator class to convert DataJoint pipeline (or portion of the pipeline) into prefect Flow
    """

    def __init__(
        self,
        flow_name,
        storage=None,
        trigger_poll_interval=600,
    ):
        self.flow_name = flow_name
        self.trigger_interval = trigger_poll_interval

        self.processes = {}
        self.tasks = {}
        self.task_count = -1
        self._terminal_process = None

        self.storage = storage or LocalFileSystem()

        self.flows, self.deployments = {}, {}

    def __call__(self, process):
        self.task_count += 1
        self.processes[self.task_count] = process
        if isinstance(process, dj.user_tables.TableMeta):

            @task(name=process.__name__)
            def flow_task(restrictions):
                job_errors = process.populate(*restrictions, **_populate_settings)
                if len(job_errors):
                    error_msg = (
                        f"Populate Task failed - {len(job_errors)} job errors.\n"
                    )
                    error_msg += "\n".join([j for _, j in job_errors])
                    # delete error jobs from job-table
                    vmod = dj.create_virtual_module(process.database, process.database)
                    (
                        vmod.schema.jobs
                        & [{"key_hash": dj.hash.key_hash(k)} for k, _ in job_errors]
                    ).delete()
                    raise dj.errors.DataJointError(error_msg)

            self.tasks[self.task_count] = flow_task
            self._terminal_process = self.task_count
        elif inspect.isfunction(process) or inspect.ismethod(process):

            @task(name=process.__name__)
            def flow_task(restrictions):
                process()

            self.tasks[self.task_count] = flow_task
        else:
            raise NotImplemented(
                f"Unable to handle processing step of type {type(process)}"
            )

    @property
    def main_flow(self):
        if "main" not in self.flows:

            @flow(name=self.flow_name)
            def _flow(keys):
                tasks_output = {}
                for task_idx, flowtask in self.tasks.items():
                    if task_idx == 0:
                        tasks_output[task_idx] = flowtask(keys)
                    else:
                        tasks_output[task_idx] = flowtask(
                            keys, wait_for=[tasks_output[task_idx - 1]]
                        )

            self.flows["main"] = _flow

        return self.flows["main"]

    @property
    def trigger_flow(self):
        """
        This flow accomplishes two goals
        1. schedule new flow runs based on the `key_source` of the parent table
        2. cancel staled scheduled runs
        """
        if "trigger" not in self.flows:
            assert "main" in self.flows

            if not isinstance(self.processes[0], dj.user_tables.TableMeta):
                return None

            @task(name=self.flow_name + "_trigger")
            def flow_trigger():
                keys_todo = (
                    self.processes[0].key_source
                    - self.processes[self._terminal_process]
                )
                keys_todo = keys_todo.fetch("KEY")

                logger.info(
                    f"Creating {len(keys_todo)} flow run(s) - Flow: {self.flows['main'].name}"
                )
                for key in keys_todo:
                    run_deployment(
                        name=f"{self.flows['main'].name}/{self.deployments['main'][-1].name}",
                        parameters={"keys": [key]},
                        flow_run_name=str(key),
                        idempotency_key=dj.hash.key_hash(key),
                        timeout=0,
                    )

            @flow(name=self.flow_name + "_trigger")
            def _trigger():
                flow_trigger()

            self.flows["trigger"] = _trigger

        return self.flows["trigger"]

    def apply(self):
        currentframe = inspect.currentframe()

        print(inspect.getouterframes(inspect.currentframe()))

        return currentframe

        #
        # main_deployment = Deployment.build_from_flow(
        #     flow=self.main_flow,
        #     name=f"{self.main_flow.name}-deployment",
        #     storage=self.storage,
        #     work_queue_name=f"{self.main_flow.name}-queue",
        # )
        # d_id = main_deployment.apply()
        # self.deployments["main"] = (d_id, main_deployment)
        #
        # trigger_deployment = Deployment.build_from_flow(
        #     flow=self.trigger,
        #     name=f"{self.trigger_flow.name}-deployment",
        #     work_queue_name=f"{self.trigger_flow.name}-queue",
        #     storage=self.storage,
        #     schedule=IntervalSchedule(interval=self.trigger_interval),
        # )
        # d_id = trigger_deployment.apply()
        # self.deployments["trigger"] = (d_id, trigger_deployment)
