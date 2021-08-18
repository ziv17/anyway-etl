import dataflows as DF
import multiprocessing as mp

from anyway_etl.waze.utils import WazeDataRetriever, WazeDataFlowsHandler


def _process_dataflow(dataflow: DF):
    dataflow.process()


def get_waze_data() -> dict:
    data_retriever, dataflows_handler = WazeDataRetriever(), WazeDataFlowsHandler()

    waze_data = data_retriever.get_data()

    dataflows = dataflows_handler.get_data_flows(waze_data)

    with mp.Pool(mp.cpu_count()) as pool:
        pool.map(_process_dataflow, dataflows)


if __name__ == "__main__":
    get_waze_data()
