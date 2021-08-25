import os
import dataflows as DF

from anyway_etl.waze.utils.parser_retriever import ParserRetriever


class DataflowBuilder:
    def __init__(self, parser_retriever: ParserRetriever):
        self.parser_retriever = parser_retriever

    @property
    def parent_directory(self):
        return os.path.dirname(os.path.dirname(__file__))

    def get_items(self, waze_data: dict, field: str) -> list:
        raw_data = waze_data.get(field, [])

        parser = self.parser_retriever.get_parser(field)

        parsed_data = parser(raw_data)

        return parsed_data

    def build_dataflow(self, waze_data: dict, field: str) -> DF.Flow:
        items = self.get_items(waze_data, field)

        output_path = os.path.join(self.parent_directory, f"waze_{field}")

        return DF.Flow(items, DF.dump_to_path(output_path))
