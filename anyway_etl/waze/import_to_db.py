import os
import dataflows as DF

from anyway_etl.waze.config import FIELDS, TYPES_MAPPING
from anyway_etl.db import session_decorator
from anyway_etl.config import ANYWAY_ETL_DATA_ROOT_PATH


def _get_saved_data(datapackage_filename):
    return DF.Flow(DF.load(datapackage_filename)).results()[0][0]


@session_decorator
def _import_data(session, field: str, data: list):
    data_type = TYPES_MAPPING[field]
    session.bulk_insert_mappings(data_type, data)
    session.commit()


def import_waze_data_to_db():
    for field in FIELDS:
        full_path = os.path.join(ANYWAY_ETL_DATA_ROOT_PATH, "waze", field)

        data_path = os.path.join(full_path, "datapackage.json")

        data = _get_saved_data(datapackage_filename=data_path)

        _import_data(field, data)


if __name__ == "__main__":
    import_waze_data_to_db()
