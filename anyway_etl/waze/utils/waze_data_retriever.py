import requests

from anyway_etl.waze.config import API_URL, API_PARAMS


class WazeDataRetriever:
    def __get_data_from_api(self) -> dict:
        response = requests.get(url=API_URL, params=API_PARAMS, verify=False)

        response_json = response.json()

        return response_json

    def get_data(self) -> dict:
        return self.__get_data_from_api()
