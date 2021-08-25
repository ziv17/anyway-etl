from anyway_etl.waze.utils.dataflow_builder import DataflowBuilder


class DataflowsHandler:
    def __init__(self, dataflow_builder: DataflowBuilder):
        self.__fields = ["alerts", "jams"]

        self.dataflow_builder = dataflow_builder

    def get_dataflows(self, waze_data: dict) -> list:
        build_dataflow = self.dataflow_builder.build_dataflow

        return [build_dataflow(waze_data, field) for field in self.__fields]
