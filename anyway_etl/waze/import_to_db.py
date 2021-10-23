import os
import dataflows as DF
import pandas as pd
import shutil
from datetime import datetime
from typing import Callable, Optional
from sqlalchemy.orm.session import Session
from pprint import pprint
from collections import defaultdict


from anyway_etl.waze.config import FIELDS, TYPES_MAPPING, QUERIES, \
    WAZE_TYPE
from anyway_etl.db import session_decorator
from anyway_etl.config import ANYWAY_ETL_DATA_ROOT_PATH


@session_decorator
def __does_exist_in_db(session: Session, select_query: str, uuid: str) -> bool:
    query_with_uuid = select_query.format(uuid=uuid)

    db_execution = session.execute(query_with_uuid)

    results = list(db_execution)

    if len(results) == 0:
        return False

    count = results[0][0]

    return count > 0


@session_decorator
def __insert_to_db(session: Session, data_type: WAZE_TYPE, row: dict) -> None:
    session.add(data_type(**row))


@session_decorator
def __update_row(session: Session, data_type: WAZE_TYPE, row: dict) -> None:
    row['update_time'] = datetime.now()

    uuid = row.get('uuid')

    session.query(data_type). \
        filter_by(uuid=str(uuid)). \
        update(row)


@session_decorator
def __commit_all_changes_to_db(session: Session) -> None:
    session.commit()


def __get_row_handler(field: str, stats: dict) -> Callable[[dict], None]:
    field_queries = QUERIES.get(field)
    select_query = field_queries.get("select_count")
    data_type = TYPES_MAPPING.get(field)

    def _handler(row: dict) -> None:
        uuid = row.get("uuid")
        exists_in_db = __does_exist_in_db(select_query, uuid)

        try:
            if exists_in_db:
                __update_row(data_type, row)
                stats['updated_rows'] += 1
            else:
                __insert_to_db(data_type, row)
                stats['inserted_rows'] += 1
        except Exception as exc:
            pprint(f'row with uuid {uuid} failed: {str(exc)}')
            stats['failed_rows'] += 1

    return _handler


def __get_insertion_flow(field: str, path: str, stats: dict) -> DF.Flow:
    handler = __get_row_handler(field, stats)

    return DF.Flow(DF.load(path), handler)


def import_waze_data_to_db() -> None:
    for field in FIELDS:
        print(f'Importing waze {field} to DB...')

        full_path = os.path.join(ANYWAY_ETL_DATA_ROOT_PATH, "waze", field)
        data_path = os.path.join(full_path, "datapackage.json")

        stats = defaultdict(int)

        dataflow = __get_insertion_flow(field, path=data_path, stats=stats)

        dataflow.process()

        print('Committing changes to DB...')

        __commit_all_changes_to_db()

        print(f'Finished importing {field}!')

        pprint(stats)

        print(f'Removing local waze {field}...')

        shutil.rmtree(path=data_path, ignore_errors=True)

        print('Removed!')
