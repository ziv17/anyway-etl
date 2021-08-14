import dataflows as DF

from anyway_etl.waze.utils.waze_parser_retriever import WazeParserRetriever


class WazeDataFlowBuilder:
    def __init__(self):
        self.__parser_retriever = WazeParserRetriever()

    def __get_items(self, waze_data: dict, field: str):
        return waze_data.get(field, [])

    def build_data_flow(self, waze_data: dict, field: str) -> DF.Flow:

        get_parser = self.__parser_retriever.get_parser

        parser = get_parser(field)

        return DF.Flow(self.__get_items(waze_data, field), parser)
