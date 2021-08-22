import dataflows as DF

class DataflowBuilder:
    def __init__(self, parser_retriever, importer_handler):
        self.parser_retriever = parser_retriever
        self.importer_handler = importer_handler

    def get_items(self, waze_data: dict, field: str) -> list:
        return waze_data.get(field, [])

    def build_dataflow(self, waze_data: dict, field: str) -> DF.Flow:
        parser = self.parser_retriever.get_parser(field)

        importer = self.importer_handler.get_importer(field)

        return DF.Flow(
            self.get_items(waze_data, field),
            parser,
            importer
        )
