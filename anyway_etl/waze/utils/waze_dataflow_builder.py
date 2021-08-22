import dataflows as DF

from anyway_etl.waze.utils.waze_parser_retriever import WazeParserRetriever
from anyway_etl.waze.utils.importer_handler import ImporterHandler


class WazeDataFlowBuilder:
    def __init__(self):
        self.__parser_retriever = WazeParserRetriever()
        self.__importer_handler = ImporterHandler()

    def __get_items(self, waze_data: dict, field: str) -> list:
        return waze_data.get(field, [])

    def build_dataflow(self, waze_data: dict, field: str) -> DF.Flow:

        get_parser, get_importer = self.__parser_retriever.get_parser, \
                self.__importer_handler.get_importer

        parser = get_parser(field)

        importer = get_importer(field)

        return DF.Flow(
            self.__get_items(waze_data, field), 
            parser,
            importer
        )
