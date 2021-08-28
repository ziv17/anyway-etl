import os
import dataflows as DF

from anyway_etl.waze.utils.parser_retriever import ParserRetriever
from anyway_etl.config import ANYWAY_ETL_DATA_ROOT_PATH


class DataflowBuilder:
    def __init__(self, parser_retriever: ParserRetriever):
        self.parser_retriever = parser_retriever

    def get_items(self, waze_data: dict, field: str) -> list:
        raw_data = waze_data.get(field, [])

        parser = self.parser_retriever.get_parser(field)

        parsed_data = parser(raw_data)

        return parsed_data

    def build_dataflow(self, waze_data: dict, field: str) -> DF.Flow:
        items = self.get_items(waze_data, field)

        output_path = os.path.join(ANYWAY_ETL_DATA_ROOT_PATH, "waze", field)

        return DF.Flow(items, DF.dump_to_path(output_path))
