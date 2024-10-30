"""
Prefect integration with DataJoint - providing a more complete workflow management experience
Requires `prefect` - see https://github.com/prefecthq/prefect
"""

import inspect
import datajoint as dj
import pathlib
import re
from datetime import datetime, timedelta

from prefect import task, flow
from prefect.deployments import run_deployment
from prefect.filesystems import LocalFileSystem
from prefect.logging import get_run_logger


_populate_settings = {
    "display_progress": True,
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
        trigger_poll_interval=600,
    ):
        self.flow_name = flow_name
        self.trigger_interval = trigger_poll_interval

        self.processes = {}
        self.tasks = {}
        self.task_count = -1
        self._terminal_process = None

        self.flows = {}

        self._main_flow_name = self.flow_name
        self._trigger_flow_name = self._main_flow_name + "_trigger"
        self._main_flow_deploy_name = self._main_flow_name + "-deploy"
        self._trigger_flow_deploy_name = self._trigger_flow_name + "-deploy"

    def __call__(self, process):
        """
        Decorator to add a processing step as one Prefect "Task" to the Prefect "Flow"
        """

        self.task_count += 1
        self.processes[self.task_count] = process
        if isinstance(process, dj.user_tables.TableMeta):

            @task(name=process.__name__)
            def flow_task(restrictions):
                logger = get_run_logger()

                status = process.populate(*restrictions, **_populate_settings)
                success_count = status["success_count"]
                error_list = status["error_list"]
                logger.info(f"Populate status - {success_count} jobs succeeded.")
                if len(error_list):
                    logger.error(f"Populate status - {len(error_list)} job errors.")
                    # delete error jobs from job-table
                    vmod = dj.create_virtual_module(process.database, process.database)
                    (
                        vmod.schema.jobs
                        & [{"key_hash": dj.hash.key_hash(k)} for k, _ in error_list]
                    ).delete()
                    error_msg = "\n".join([j for _, j in error_list])
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
        """
        Assemble the primary (main) Prefect "Flow" from all the tasks
        """

        if "main" not in self.flows:

            @flow(name=self._main_flow_name)
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
        Creates a secondary Flow (named "trigger") to schedule new flow-runs of the main flow
        This flow accomplishes two goals
        1. schedule new flow runs based on the `key_source` of the parent table
        2. cancel staled scheduled runs
        """
        if "trigger" not in self.flows:
            assert "main" in self.flows

            if not isinstance(self.processes[0], dj.user_tables.TableMeta):
                return None

            @task(name="create_main_runs")
            def create_main_runs():
                logger = get_run_logger()

                keys_todo = (
                    self.processes[0].key_source
                    - self.processes[self._terminal_process]
                )
                keys_todo = keys_todo.fetch("KEY")

                # Queue up flow runs for main_flow
                logger.info(
                    f"Creating {len(keys_todo)} flow run(s) - Flow: {self.flows['main'].name}"
                )
                for key in keys_todo:
                    run_deployment(
                        name=f"{self._main_flow_name}/{self._main_flow_deploy_name}",
                        parameters={"keys": [key]},
                        flow_run_name=str(key),
                        idempotency_key=dj.hash.key_hash(key),
                        timeout=0,
                    )
            #
            # @task(name="create_trigger_runs")
            # def create_trigger_run():
            #     # Queue up one next flow run for trigger_flow
            #     run_deployment(
            #         name=f"{self._trigger_flow_name}/{self._trigger_flow_deploy_name}",
            #         timeout=0,
            #         scheduled_time=datetime.utcnow()
            #         + timedelta(seconds=self.trigger_interval),
            #     )

            @flow(name=self._trigger_flow_name)
            def _trigger():
                create_main_runs()
                # create_trigger_run()

            self.flows["trigger"] = _trigger

        return self.flows["trigger"]

    def deploy(self, source=None, image=None, build=False, push=False, work_queue_name=None, work_pool_name=None):
        source = source or LocalFileSystem()

        calling_frame = inspect.getouterframes(inspect.currentframe())[1]
        datajoint_flow_name = re.search(
            r"\s?(\S+)\.deploy\(.*", calling_frame.code_context[0]
        ).groups()[0]

        calling_package = calling_frame.frame.f_globals["__package__"]
        calling_file = pathlib.Path(calling_frame.frame.f_globals["__file__"]).name

        self.flows["main"].from_source(
            source=source,
            entrypoint=f"{calling_package.replace('.', '/')}/{calling_file}:{datajoint_flow_name}_main_flow",
        ).deploy(
            name=self._main_flow_deploy_name,
            work_queue_name=work_queue_name or f"{self.main_flow.name}-queue",
            work_pool_name=work_pool_name or f"{self.main_flow.name}-work-pool",
            image=image,
            build=False,
            push=False
        )

        self.flows["trigger"].from_source(
            source=source,
            entrypoint=f"{calling_package.replace('.', '/')}/{calling_file}:{datajoint_flow_name}_trigger_flow",
        ).deploy(
            name=self._trigger_flow_deploy_name,
            work_queue_name=work_queue_name or f"{self.main_flow.name}-queue",
            work_pool_name=work_pool_name or f"{self.main_flow.name}-work-pool",
            cron="*/10 * * * *",
            image=image,
            build=False,
            push=False
        )
