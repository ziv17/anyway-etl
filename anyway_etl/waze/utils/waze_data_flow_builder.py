import dataflows as DF

from anyway_etl.waze.utils.waze_parser_retriever import WazeParserRetriever


class WazeDataFlowBuilder:
    def __init__(self):
        self.__parser_retriever = WazeParserRetriever()

    def __yield_items(self, waze_data: dict, field: str):
        waze_items = waze_data.get(field, [])

        for waze_item in waze_items:
            yield waze_item

    def build_data_flow(self, waze_data: dict, field: str) -> DF.Flow:

        get_parser = self.__parser_retriever.get_parser

        return DF.Flow(
            self.__yield_items(waze_data, field), DF.parallelize(get_parser(field))
        )
