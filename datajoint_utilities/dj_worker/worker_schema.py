import json
import os
import platform
import traceback
import pymysql
import inspect
import re
from datetime import datetime
import datajoint as dj
import numpy as np
import pandas as pd

from datajoint.user_tables import Part, UserTable
from datajoint.condition import AndList

logger = dj.logger


class RegisteredWorker(dj.Manual):
    definition = """
    worker_name: varchar(64)
    ---
    registration_time: datetime(6) # registration datetime (UTC)
    worker_kwargs: longblob  # keyword arguments to instantiate a DataJointWorker class
    worker_config_uuid: uuid
    unique index (worker_name, worker_config_uuid)
    """

    class Process(dj.Part):
        definition = """
        -> master
        process_index: int  # running order for this process from this worker
        ---
        process: varchar(64)
        full_table_name='': varchar(255)  # full table name if this process is a DJ table
        key_source_sql=null  :longblob  # sql statement for the key_source of the table - from make_sql()
        process_kwargs: longblob  # keyword arguments to pass when calling the process
        process_config_uuid: UUID # hash of all attributes defining this process
        """

    @classmethod
    def get_workers_progress(cls, worker_name: str = None, process_name: str = None, schedule_jobs: bool = True) -> pd.DataFrame:
        """
        Get the operation progress for all registered workers, showing job status for each AutoPopulate process.

        This method aggregates information about:
        - Total and incomplete entries in key_source tables
        - Number of reserved, error, and ignored jobs
        - Remaining jobs to be processed

        Args:
            worker_name (str, optional): Filter results by specific worker name. Defaults to None.
            process_name (str, optional): Filter results by specific process name. Defaults to None.
            schedule_jobs (bool, optional): If True, schedule new jobs for incomplete key_source entries. Defaults to True.
            
        Returns:
            pd.DataFrame: DataFrame containing workflow status with columns:
                - total: Total number of entries in key_source
                - incomplete: Number of incomplete entries
                - reserved: Number of reserved jobs
                - error: Number of error jobs
                - ignore: Number of ignored jobs
                - remaining: Number of remaining jobs to process
        """
        restriction = {}
        if worker_name:
            restriction["worker_name"] = worker_name
        if process_name:
            restriction["process"] = process_name

        workflow_status = (
            (
                (cls.Process & "key_source_sql is not NULL").proj(
                    "process",
                    "key_source_sql",
                    table_name="full_table_name",
                    total="NULL",
                    incomplete="NULL",
                )
                & restriction
            )
            .fetch(format="frame")
            .reset_index()
        )
        workflow_status.drop_duplicates(subset=["worker_name", "process"], inplace=True)

        # extract jobs status
        schema_names = set(
            [n.split(".")[0].strip("`") for n in workflow_status.table_name if n]
        )
        pipeline_schemas = {
            n: dj.Schema(n, connection=cls.connection, create_schema=False, create_tables=False)
            for n in schema_names
        }

        job_status_df = {"reserved": [], "error": [], "ignore": []}
        for pipeline_schema in pipeline_schemas.values():
            try:
                len(pipeline_schema.jobs)
            except dj.errors.DataJointError:
                continue
            for job_status in ("reserved", "error", "ignore"):
                status_df = (
                    dj.U("table_name")
                    .aggr(
                        pipeline_schema.jobs & f'status = "{job_status}"',
                        **{job_status: "count(table_name)"},
                    )
                    .fetch(format="frame")
                )
                status_df.index = status_df.index.map(
                    lambda x: f"{pipeline_schema.database}.{x}"
                )
                job_status_df[job_status].append(status_df)

        for k, v in job_status_df.items():
            job_status_df[k] = pd.concat(v)

        # extract AutoPopulate key_source status
        for r_idx, r in workflow_status.iterrows():
            if not r.key_source_sql:
                continue
            if schedule_jobs:
                cls.schedule_jobs(r.worker_name, r.process)
            (
                workflow_status.loc[r_idx, "total"],
                workflow_status.loc[r_idx, "incomplete"],
            ) = cls.get_key_source_count(r.key_source_sql, r.table_name)

        # merge key_source and jobs status
        workflow_status.set_index("table_name", inplace=True)
        workflow_status.index = workflow_status.index.map(lambda x: x.replace("`", ""))

        workflow_status = workflow_status.join(
            job_status_df["reserved"]
            .join(job_status_df["error"], how="outer")
            .join(job_status_df["ignore"], how="outer"),
            how="left",
        )

        workflow_status.fillna(0, inplace=True)
        workflow_status.replace(np.inf, np.nan, inplace=True)

        workflow_status["remaining"] = (
            workflow_status.incomplete
            - workflow_status.reserved
            - workflow_status.error
            - workflow_status.ignore
        )

        workflow_status.set_index("process", inplace=True)
        workflow_status.drop(columns=["process_index", "key_source_sql"], inplace=True)
        return workflow_status

    @classmethod
    def get_incomplete_key_source_sql(cls, key_source_sql: str, target_full_table_name: str) -> tuple[str, str]:
        """
        Build a SQL statement to find incomplete key_source entries in the target table.

        This method constructs a SQL query that identifies entries in the key_source that
        have not yet been processed into the target table.

        Args:
            key_source_sql (str): Original SQL statement for the key_source of the table
            target_full_table_name (str): Full table name of the target table (including schema)

        Returns:
            tuple[str, str]: A tuple containing:
                - SQL statement for total key_source entries
                - SQL statement for incomplete key_source entries
        """

        def _rename_attributes(table, props):
            return (
                table.proj(
                    **{
                        attr: ref
                        for attr, ref in props["attr_map"].items()
                        if attr != ref
                    }
                )
                if props["aliased"]
                else table.proj()
            )

        target = dj.FreeTable(
            full_table_name=target_full_table_name, conn=cls.connection
        )

        try:
            len(target)
        except dj.errors.DataJointError:
            return np.nan, np.nan

        parents = target.parents(primary=True, as_objects=True, foreign_key_info=True)

        ks_parents = _rename_attributes(*parents[0])
        for q in parents[1:]:
            ks_parents *= _rename_attributes(*q)

        ks_attrs_sql = ks_parents.heading.as_sql(ks_parents.heading.primary_key)
        AND_or_WHERE = (
            "AND"
            if "WHERE" in _remove_enclosed_parentheses(key_source_sql)
            else " WHERE "
        )
        incomplete_sql = (
            key_source_sql
            + f"{AND_or_WHERE}(({ks_attrs_sql}) not in (SELECT {ks_attrs_sql} FROM {target.full_table_name}))"
        )
        return incomplete_sql

    @classmethod
    def get_key_source_count(
        cls,
        key_source_sql: str,
        target_full_table_name: str,
        andlist_restriction: AndList = None,
        return_sql: bool = False
    ) -> tuple[int, int] | tuple[str, str]:
        """
        Count total and incomplete key_source entries in the target table.

        This method executes SQL queries to count:
        1. Total number of entries in the key_source
        2. Number of entries that haven't been processed into the target table

        Args:
            key_source_sql (str): SQL statement for the key_source of the table
            target_full_table_name (str): Full table name of the target table
            andlist_restriction (AndList, optional): Additional restrictions to add to the queries. Defaults to None.
            return_sql (bool, optional): If True, return SQL statements instead of counts. Defaults to False.

        Returns:
            tuple[int, int] | tuple[str, str]: If return_sql is False:
                - (total_count, incomplete_count)
              If return_sql is True:
                - (total_sql, incomplete_sql)
        """
        incomplete_sql = cls.get_incomplete_key_source_sql(
            key_source_sql, target_full_table_name
        )

        if andlist_restriction:
            restriction_str = ")AND(".join(str(s) for s in andlist_restriction)

            AND_or_WHERE = (
                "AND"
                if "WHERE" in _remove_enclosed_parentheses(key_source_sql)
                else " WHERE "
            )

            key_source_sql += f" {AND_or_WHERE} ({restriction_str})"
            incomplete_sql += f" AND ({restriction_str})"

        if return_sql:
            return key_source_sql, incomplete_sql

        try:
            total = len(cls.connection.query(key_source_sql).fetchall())
            incomplete = len(cls.connection.query(incomplete_sql).fetchall())
        except Exception as e:
            logger.error(
                f"Error retrieving key_source for: {target_full_table_name}. \n{e}"
            )
            total, incomplete = np.nan, np.nan
        return total, incomplete

    @classmethod
    def schedule_jobs(
        cls,
        worker_name: str,
        process_name: str,
        min_scheduling_interval: int = None
    ) -> int:
        """
        Schedule new jobs for a specific worker's process based on incomplete key source entries.
        
        This method:
        1. Finds the registered process for the specified worker and process name
        2. Gets incomplete key source entries
        3. Schedules new jobs for those entries
        
        Args:
            worker_name (str): Name of the worker to schedule jobs for
            process_name (str): Name of the process to schedule jobs for
            min_scheduling_interval (int, optional): Minimum time in seconds that must have passed since last job scheduling.
                If None, uses the value from dj.config["min_scheduling_interval"]. Defaults to None.
        
        Returns:
            int: Number of jobs scheduled
            
        Raises:
            ValueError: If worker_name or process_name is not found in registered processes
        """
        if min_scheduling_interval is None:
            min_scheduling_interval = dj.config["min_scheduling_interval"]

        # Find the registered process for this worker and process
        try:
            process = (
                cls.Process
                & {"worker_name": worker_name, "process": process_name}
                & "key_source_sql is not NULL"
            ).fetch1()
        except dj.errors.DataJointError:
            raise ValueError(
                f"Process '{process_name}' not found for worker '{worker_name}'"
            )

        # Get schema and jobs table
        target_full_table_name = process["full_table_name"]
        schema_name = target_full_table_name.split(".")[0].strip("`")
        table_name = target_full_table_name.split(".")[1].strip("`")
        
        try:
            schema = dj.Schema(schema_name, connection=cls.connection, create_schema=False, create_tables=False)
            jobs_table = schema.jobs

            # Define scheduling event
            __scheduled_event = {
                "table_name": f"__{table_name}__",
                "__type__": "jobs scheduling event"
            }

            # First check if we have any recent jobs
            if min_scheduling_interval > 0:
                recent_scheduling_event = (
                    jobs_table.proj(last_scheduled="TIMESTAMPDIFF(SECOND, timestamp, UTC_TIMESTAMP())")
                    & {"table_name": __scheduled_event["table_name"]}
                    & {"key_hash": dj.hash.key_hash(__scheduled_event)}
                    & f"last_scheduled <= {min_scheduling_interval}"
                )
                if recent_scheduling_event:
                    logger.info(
                        f"Skip jobs scheduling for worker '{worker_name}', process '{process_name}' "
                        f"(last scheduled {recent_scheduling_event.fetch1('last_scheduled')} seconds ago)"
                    )
                    return 0

            # Get incomplete key source entries and schedule jobs
            incomplete_sql = cls.get_incomplete_key_source_sql(
                process["key_source_sql"],
                target_full_table_name
            )
            
            schedule_count = 0
            with cls.connection.transaction:
                for key in cls.connection.query(incomplete_sql).fetch(as_dict=True):
                    schedule_count += jobs_table.schedule(table_name, key)
                
                # Record scheduling event
                jobs_table.ignore(
                    __scheduled_event["table_name"],
                    __scheduled_event,
                    message=f"Jobs scheduling event: {__scheduled_event['table_name']}"
                )
            
            logger.info(
                f"{schedule_count} new jobs scheduled for worker '{worker_name}', process '{process_name}'"
            )
            
            return schedule_count

        except dj.errors.DataJointError as e:
            logger.error(f"Error accessing schema {schema_name}: {str(e)}")
            return 0


class WorkerLog(dj.Manual):
    definition = """
    # Registration of processing jobs running .populate() jobs or custom function
    process_timestamp : datetime(6)   # timestamp of the processing job (UTC)
    process           : varchar(64)
    ---
    worker_name=''    : varchar(255)  # name of the worker
    host              : varchar(255)  # system hostname
    user=''           : varchar(255)  # database user
    pid=0             : int unsigned  # system process id
    """

    _table_name = "~worker_log"

    @classmethod
    def log_process_job(cls, process, worker_name="", db_prefix=("",)):
        process_name = get_process_name(process, db_prefix)
        user = cls.connection.get_user()

        if not worker_name:
            frame = inspect.currentframe()
            function_name = frame.f_back.f_code.co_name
            module_name = inspect.getmodule(frame.f_back).__name__
            worker_name = f"{module_name}.{function_name}"

        cls.insert1(
            {
                "process": process_name,
                "process_timestamp": datetime.utcnow(),
                "worker_name": worker_name,
                "host": platform.node(),
                "user": user,
                "pid": os.getpid(),
            }
        )

    @classmethod
    def get_recent_jobs(cls, backtrack_minutes=60):
        recent = (
            cls.proj(
                minute_elapsed="TIMESTAMPDIFF(MINUTE, process_timestamp, UTC_TIMESTAMP())"
            )
            & f"minute_elapsed < {backtrack_minutes}"
        )

        recent_jobs = dj.U("process").aggr(
            cls & recent,
            worker_count="count(DISTINCT pid)",
            minutes_since_oldest="TIMESTAMPDIFF(MINUTE, MIN(process_timestamp), UTC_TIMESTAMP())",
            minutes_since_newest="TIMESTAMPDIFF(MINUTE, MAX(process_timestamp), UTC_TIMESTAMP())",
        )

        return recent_jobs

    @classmethod
    def delete_old_logs(cls, cutoff_days=30):
        # if latest log is older than cutoff_days, then do nothing
        old_jobs = (
            cls.proj(
                elapsed_days=f'TIMESTAMPDIFF(DAY, process_timestamp, "{datetime.utcnow()}")'
            )
            & f"elapsed_days > {cutoff_days}"
        )
        if old_jobs:
            with dj.config(safemode=False):
                try:
                    (cls & old_jobs).delete_quick()
                except pymysql.err.OperationalError:
                    pass


class ErrorLog(dj.Manual):
    definition = """
    # Logging of job errors
    process           : varchar(64)
    key_hash          : char(32)      # key hash
    ---
    error_timestamp   : datetime(6)   # timestamp of the processing job (UTC)
    key               : varchar(2047) # structure containing the key
    error_message=""  : varchar(2047) # error message returned if failed
    error_stack=null  : mediumblob    # error stack if failed
    host              : varchar(255)  # system hostname
    user=''           : varchar(255)  # database user
    pid=0             : int unsigned  # system process id
    """

    _table_name = "~error_log"

    @classmethod
    def log_error_job(cls, error_entry, schema_name, db_prefix=("",)):
        # if the exact same error has been logged, just update the error record

        table_name = error_entry["table_name"]
        schema_name = re.sub("|".join(db_prefix), "", schema_name.strip("`"))
        table_name = dj.utils.to_camel_case(table_name.strip("`"))
        process_name = f"{schema_name}.{table_name}"

        entry = {
            "process": process_name,
            "key_hash": error_entry["key_hash"],
            "error_timestamp": error_entry["timestamp"],
            "key": json.dumps(error_entry["key"], default=str),
            "error_message": error_entry["error_message"],
            "error_stack": error_entry["error_stack"],
            "host": error_entry["host"],
            "user": error_entry["user"],
            "pid": error_entry["pid"],
        }

        if cls & {"process": entry["process"], "key_hash": entry["key_hash"]}:
            cls.update1(entry)
        else:
            cls.insert1(entry)

    @classmethod
    def log_exception(cls, key, process, error):
        error_message = "{exception}{msg}".format(
            exception=error.__class__.__name__,
            msg=": " + str(error) if str(error) else "",
        )
        entry = {
            "process": process.__name__,
            "key_hash": dj.hash.key_hash(key),
            "error_timestamp": datetime.utcnow(),
            "key": json.dumps(key, default=str),
            "error_message": error_message,
            "error_stack": traceback.format_exc(),
            "host": platform.node(),
            "user": cls.connection.get_user(),
            "pid": os.getpid(),
        }

        if cls & {"process": entry["process"], "key_hash": entry["key_hash"]}:
            cls.update1(entry)
        else:
            cls.insert1(entry)

    @classmethod
    def delete_old_logs(cls, cutoff_days=30):
        old_jobs = (
            cls.proj(
                elapsed_days=f'TIMESTAMPDIFF(DAY, error_timestamp, "{datetime.utcnow()}")'
            )
            & f"elapsed_days > {cutoff_days}"
        )
        if old_jobs:
            with dj.config(safemode=False):
                try:
                    (cls & old_jobs).delete_quick()
                except pymysql.err.OperationalError:
                    pass


def get_process_name(process, db_prefix):
    if is_djtable(process):
        schema_name, table_name = process.full_table_name.split(".")
        schema_name = re.sub("|".join(db_prefix), "", schema_name.strip("`"))
        table_name = dj.utils.to_camel_case(table_name.strip("`"))
        process_name = f"{schema_name}.{table_name}"
    elif inspect.isfunction(process) or inspect.ismethod(process):
        process_name = process.__name__
    else:
        raise ValueError("Input process must be either a DataJoint table or a function")
    return process_name


def is_djtable(obj, base_class=None) -> bool:
    if base_class is None:
        base_class = UserTable
    return isinstance(obj, base_class) or (
        inspect.isclass(obj) and issubclass(obj, base_class)
    )


def is_djparttable(obj) -> bool:
    return is_djtable(obj, Part)


def _remove_enclosed_parentheses(input_string):
    pattern = r"\([^()]*\)"
    # Use a while loop to recursively remove nested parentheses
    while re.search(pattern, input_string):
        # Replace all occurrences of the pattern with an {}
        input_string = re.sub(pattern, "{}", input_string)
    return input_string
