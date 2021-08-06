import requests
from pprint import pprint

from anyway_etl.waze.config import API_URL, API_PARAMS


def get_data_from_api() -> dict:
    response = requests.get(url=API_PARAMS, params=API_PARAMS, verify=False)

    response_json = response.json()

    pprint(response_json)

    return response_json
