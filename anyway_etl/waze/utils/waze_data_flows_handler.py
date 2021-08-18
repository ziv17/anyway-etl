from .waze_data_flow_builder import WazeDataFlowBuilder


class WazeDataFlowsHandler:
    def __init__(self):
        self.__fields = ["alerts", "jams"]
        self.__data_flow_builder = WazeDataFlowBuilder()

    def get_data_flows(self, waze_data: dict) -> list:
        return [
            self.__data_flow_builder.build_data_flow(waze_data, field)
            for field in self.__fields
        ]
