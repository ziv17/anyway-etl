import requests

from anyway_etl.waze.config import API_URL, API_PARAMS, REQUEST_TIMEOUT_IN_SECONDS


class DataRetriever:
    def __get_data_from_api(self) -> dict:
        response = requests.get(url=API_URL, params=API_PARAMS, verify=False,
                                timeout=REQUEST_TIMEOUT_IN_SECONDS)

        response_json = response.json()

        return response_json

    def get_data(self) -> dict:
        return self.__get_data_from_api()
