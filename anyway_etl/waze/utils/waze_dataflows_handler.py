from .waze_dataflow_builder import WazeDataFlowBuilder


class WazeDataFlowsHandler:
    def __init__(self):
        self.__fields = ["alerts", "jams"]
        self.__dataflow_builder = WazeDataFlowBuilder()

    def get_dataflows(self, waze_data: dict) -> list:
        return [
            self.__dataflow_builder.build_dataflow(waze_data, field)
            for field in self.__fields
        ]
