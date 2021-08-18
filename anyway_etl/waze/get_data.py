from anyway_etl.waze.utils import WazeDataRetriever, WazeDataFlowsHandler


def get_waze_data() -> dict:
    data_retriever, dataflows_handler = WazeDataRetriever(), WazeDataFlowsHandler()

    waze_data = data_retriever.get_data()

    dataflows = dataflows_handler.get_data_flows(waze_data)

    for dataflow in dataflows:
        dataflow.process()


if __name__ == "__main__":
    get_waze_data()
