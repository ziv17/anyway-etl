import os
import dataflows as DF
import pandas as pd
from sqlalchemy.orm.session import Session

from anyway_etl.waze.config import FIELDS, TYPES_MAPPING, UUID
from anyway_etl.db import session_decorator
from anyway_etl.config import ANYWAY_ETL_DATA_ROOT_PATH


def _get_saved_data(datapackage_filename) -> list:
    return DF.Flow(DF.load(datapackage_filename)).results()[0][0]


@session_decorator
def _import_data(session: Session, field: str, data: list):
    data_type = TYPES_MAPPING[field]
    session.bulk_insert_mappings(data_type, data)
    session.commit()


@session_decorator
def _get_existing_data_insertion_times(
    session: Session, field: str, data: list
) -> dict:
    data_type = TYPES_MAPPING[field]

    uuids_from_waze_feed = [str(item.get(UUID)) for item in data]

    uuids_from_waze_feed = list(set(uuids_from_waze_feed))

    query = session.query(data_type).filter(
        getattr(data_type, UUID).in_(uuids_from_waze_feed)
    )

    data_from_db: pd.DataFrame = pd.read_sql_query(query.statement, query.session.bind)

    uuid_to_insertion_time = {
        row.uuid: row.insertion_time for row in data_from_db.itertuples()
    }

    session.query(data_type).filter(
        getattr(data_type, UUID).in_(uuids_from_waze_feed)
    ).delete(synchronize_session="fetch")

    session.commit()

    return uuid_to_insertion_time


def _set_insertion_times(data: list, uuid_to_insertion_time: dict) -> list:
    df = pd.DataFrame(data)

    df.set_index(UUID, inplace=True)

    df.index = df.index.map(str)

    for uuid, insertion_time in uuid_to_insertion_time.items():
        df.loc[uuid, "insertion_time"] = insertion_time

    df.reset_index(inplace=True)

    df.rename(columns={"index": UUID}, inplace=True)

    return df.to_dict("records")


def import_waze_data_to_db():
    for field in FIELDS:
        full_path = os.path.join(ANYWAY_ETL_DATA_ROOT_PATH, "waze", field)

        data_path = os.path.join(full_path, "datapackage.json")

        data = _get_saved_data(datapackage_filename=data_path)

        existing_uuids_and_insertion_times = _get_existing_data_insertion_times(
            field, data
        )

        data = _set_insertion_times(
            data, uuid_to_insertion_time=existing_uuids_and_insertion_times
        )

        _import_data(field, data)


if __name__ == "__main__":
    import_waze_data_to_db()
