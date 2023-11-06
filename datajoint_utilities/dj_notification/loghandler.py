from logging import StreamHandler
import re
import datajoint as dj
import json


logger = dj.logger


class PopulateHandler(StreamHandler):
    """
    Custom Log Handler to parse and handle DataJoint logs related to populating tables.
    """

    _patterns = ("Making", "Error making", "Success making")

    def __init__(self, notifiers, notif_config=None, **kwargs):
        """
        :param notifiers: list of instantiated "Notifier"
        :param notif_config: a dict with keys being the table_full_names and value as following dict {'start': True, 'success': True, 'error': True}
        """

        StreamHandler.__init__(self)
        assert all(hasattr(notifier, "notify") for notifier in notifiers)
        self.notifiers = notifiers

        if notif_config is None and "full_table_names" in kwargs:
            logger.warning(
                "The arguments `full_table_names`, `on_start`, `on_success`, `on_error` are depreciated. Use `notif_config` argument instead (see doctring for more info)"
            )
            notif_config = {
                n: {
                    "start": kwargs["on_start"],
                    "success": kwargs["on_success"],
                    "error": kwargs["on_error"],
                }
                for n in kwargs["full_table_names"]
            }

        assert notif_config, "`notif_config` must be specified"
        self.notif_config = notif_config

    def emit(self, record):
        msg = self.format(record)
        if not any(p in msg for p in self._patterns):
            return
        match = re.search(
            r"(Making|Success making|Error making) (.*) -> (\S+)( - .*)?", msg
        )
        status, key_str, full_table_name, error_message = match.groups()

        key = json.loads(key_str.replace("'", '"'))
        error_message = error_message.replace(" - ", "") if error_message else ""

        status = {
            "Making": "start",
            "Success making": "success",
            "Error making": "error",
        }[status]

        if full_table_name not in self.notif_config or not self.notif_config.get(
            full_table_name, {}
        ).get(status, False):
            return

        schema_name, table_name = full_table_name.split(".")
        schema_name = schema_name.strip("`")
        table_name = dj.utils.to_camel_case(table_name.strip("`"))

        for notifier in self.notifiers:
            notifier.notify(
                title=f"DataJoint populate - {schema_name}.{table_name} - {status.upper()}",
                message=msg,
                schema_name=schema_name,
                table_name=table_name,
                key=key,
                status=status.upper(),
                error_message=error_message,
                **key,
            )
