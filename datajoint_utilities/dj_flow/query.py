import click
import pendulum
from tabulate import tabulate
from prefect.client import Client
from prefect.utilities.graphql import EnumValue, with_args


def get_flow_id(flow_name, project_name):
    query_results = {
        "name": True,
        "version": True,
        "created": True,
        "id": True,
        "project": {"name": True},
    }

    query = {
        "query": {
            with_args(
                "flow",
                {
                    "where": {
                        "_and": {
                            "name": {"_eq": flow_name},
                            "project": {"name": {"_eq": project_name}},
                        }
                    },
                    "order_by": {
                        "name": EnumValue("asc"),
                        "version": EnumValue("desc"),
                    },
                    "distinct_on": EnumValue("name"),
                    "limit": 10,
                },
            ): query_results
        }
    }

    result = Client().graphql(query)

    flow_data = result.data.flow

    assert len(flow_data) == 1

    return flow_data[0].id


def get_flow_runs(flow_name, project_name, state='scheduled', limit=100):

    order = {"created": EnumValue("desc")}

    where = {
        "flow": {
            "_and": {"name": {"_eq": flow_name},
                     "project": {"name": {"_eq": project_name}}
                     }
        },
        "state": { "_eq": state }
    }

    query = {
        "query": {
            with_args(
                "flow_run", {"where": where, "limit": limit, "order_by": order},
            ): {
                "flow": {"name": True},
                "id": True,
                "created": True,
                "state": True,
                "name": True,
                "start_time": True,
            }
        }
    }

    result = Client().graphql(query)

    flow_run_data = result.data.flow_run
    return flow_run_data

