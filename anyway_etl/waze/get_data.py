from anyway_etl.waze.utils.parser_retriever import ParserRetriever
from anyway_etl.waze.utils.dataflow_builder import DataflowBuilder
from anyway_etl.waze.utils.dataflows_handler import DataflowsHandler
from anyway_etl.waze.utils.data_retriever import DataRetriever


def get_waze_data():
    parser_retriever = ParserRetriever()
    dataflow_builder = DataflowBuilder(parser_retriever)
    dataflows_handler = DataflowsHandler(dataflow_builder)
    data_retriever = DataRetriever()

    waze_data = data_retriever.get_data()

    dataflows = dataflows_handler.get_dataflows(waze_data)

    for dataflow in dataflows:
        dataflow.process()


if __name__ == "__main__":
    get_waze_data()
