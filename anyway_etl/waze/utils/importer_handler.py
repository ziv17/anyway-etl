from anyway_etl.db import session_decorator
from anyway.models import WazeAlert, WazeTrafficJams

class ImporterHandler:

    def __init__(self):
        self.__data_types_mapping = {
            'alerts': WazeAlert,
            'jams': WazeTrafficJams
        }

    def get_importer(self, field: str):
        data_type = self.__data_types_mapping[field]

        @session_decorator
        def import_data_to_db(session, data):
            session.bulk_insert_mappings(data_type, data)

        return import_data_to_db 



