import requests

from anyway_etl.waze.utils import WazeDataRetriever, WazeDataFlowsHandler


def get_waze_data() -> dict:
    data_retriever, data_flows_handler = WazeDataRetriever(), WazeDataFlowsHandler()

    waze_data = data_retriever.get_data()

    data_flows = data_flows_handler.get_data_flows(waze_data)

    results = [data_flow.process() for data_flow in data_flows]

    print()


if __name__ == "__main__":
    get_waze_data()
