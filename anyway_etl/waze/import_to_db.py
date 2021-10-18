import os
import dataflows as DF
import pandas as pd
from datetime import datetime
from typing import Callable
from sqlalchemy.orm.session import Session

from anyway_etl.waze.config import FIELDS, TYPES_MAPPING, QUERIES
from anyway_etl.db import session_decorator
from anyway_etl.config import ANYWAY_ETL_DATA_ROOT_PATH


@session_decorator
def __does_uuid_exist(session: Session, select_count_query: str, uuid: str) -> bool:
    query_with_uuid = select_count_query.format(uuid=uuid)

    db_execution = session.execute(query_with_uuid)

    results = list(db_execution)

    if len(results) == 0:
        return False

    count = results[0][0]

    return count > 0


@session_decorator
def __insert_to_db(session: Session, insert_query: str, row: dict) -> None:
    row.pop("uuid", None)

    insert_query_with_values = insert_query.format(**row)

    session.execute(insert_query_with_values)

    session.commit()


@session_decorator
def __update_row(session: Session, update_query: str, row: dict) -> None:
    row.pop("uuid", None)

    row.pop("id", None)

    row.pop("insertion_time", None)

    row["update_time"] = datetime.now()

    update_query_with_values = update_query.format(**row)

    session.execute(update_query_with_values)

    session.commit()


def __get_row_handler(field: str) -> Callable[[dict], None]:
    field_queries = QUERIES.get(field)

    select_count_query = field_queries.get("select_count")

    insert_query = field_queries.get("insert")

    update_query = field_queries.get("update")

    def _handler(row: dict) -> None:
        uuid = row.get("uuid")
        uuid_exists = __does_uuid_exist(select_count_query, uuid)

        if uuid_exists:
            __update_row(update_query, row)
        else:
            __insert_to_db(insert_query, row)

    return _handler


def __get_insertion_flow(field: str) -> DF.Flow:
    full_path = os.path.join(ANYWAY_ETL_DATA_ROOT_PATH, "waze", field)

    data_path = os.path.join(full_path, "datapackage.json")

    handler = __get_row_handler(field)

    return DF.Flow(DF.load(data_path), handler)


def import_waze_data_to_db():
    for field in FIELDS:

        flow = __get_insertion_flow(field)

        flow.process()
