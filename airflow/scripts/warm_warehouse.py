"""Start the Databricks SQL warehouse and block until it is RUNNING.

Run before dbt so the first query never hits a cold (stopped) warehouse.
"""

import os
from datetime import timedelta

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.sql import State

WAREHOUSE_ID = os.environ.get("DATABRICKS_WAREHOUSE_ID", "5cf4b93778132e09")


def main() -> None:
    ws = WorkspaceClient(profile=os.environ.get("DATABRICKS_PROFILE", "velo"))
    wh = ws.warehouses.get(WAREHOUSE_ID)
    print(f"Warehouse '{wh.name}' is {wh.state.value if wh.state else 'UNKNOWN'}")

    if wh.state in (State.STOPPED, State.STOPPING, State.DELETED, None):
        print("Starting warehouse...")
        ws.warehouses.start(WAREHOUSE_ID)

    ws.warehouses.wait_get_warehouse_running(
        WAREHOUSE_ID, timeout=timedelta(minutes=20)
    )
    print("Warehouse is RUNNING - safe to run dbt.")


if __name__ == "__main__":
    main()
