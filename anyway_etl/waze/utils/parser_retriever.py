import pandas as pd
from datetime import datetime

from anyway.models import WazeAlert, WazeTrafficJams
from anyway_etl.waze.config import COLUMNS_MAPPING, ALERTS, JAMS


def _parse_alerts(rows):
    columns_mapping = COLUMNS_MAPPING[ALERTS]

    alerts_df = pd.json_normalize(rows)

    alerts_df["created_at"] = pd.to_datetime(alerts_df["pubMillis"], unit="ms")

    alerts_df.rename(
        {
            "location.x": "longitude",
            "location.y": "latitude",
            "nThumbsUp": "number_thumbs_up",
            "reportRating": "report_rating",
            "reportDescription": "report_description",
            "reportByMunicipalityUser": "report_by_municipality_user",
            "jamUuid": "jam_uuid",
            "type": "alert_type",
            "subtype": "alert_subtype",
            "roadType": "road_type",
        },
        axis=1,
        inplace=True,
    )

    alerts_df["geom"] = alerts_df.apply(
        lambda row: "POINT({} {})".format(row["longitude"], row["latitude"]), axis=1
    )

    for column, column_config in columns_mapping.items():
        default_value = column_config["default"]
        data_type = column_config["type"]
        conversion_func = column_config["conversion"]

        if column not in alerts_df.columns:
            alerts_df[column] = default_value

        alerts_df[column] = (
            alerts_df[column]
            .fillna(default_value)
            .apply(conversion_func)
            .astype(data_type)
        )

    alerts_df.drop(["country", "pubMillis"], axis=1, inplace=True, errors="ignore")

    for key in alerts_df.keys():
        if alerts_df[key] is None or key not in [
            field.name for field in WazeAlert.__table__.columns
        ]:
            alerts_df.drop([key], axis=1, inplace=True)

    alerts_df["insertion_time"] = datetime.now()
    alerts_df["update_time"] = datetime.now()

    return alerts_df.to_dict("records")


def _parse_jams(rows):
    jams_df = pd.json_normalize(rows)
    jams_df["created_at"] = pd.to_datetime(jams_df["pubMillis"], unit="ms")
    jams_df["geom"] = jams_df["line"].apply(
        lambda l: "LINESTRING({})".format(
            ",".join(["{} {}".format(nz["x"], nz["y"]) for nz in l])
        )
    )
    jams_df["line"] = jams_df["line"].apply(str)
    jams_df["segments"] = jams_df["segments"].apply(str)
    jams_df["turnType"] = jams_df["roadType"].fillna(-1)
    jams_df.drop(["country", "pubMillis"], axis=1, inplace=True)
    jams_df.rename(
        {
            "speedKMH": "speed_kmh",
            "turnType": "turn_type",
            "roadType": "road_type",
            "endNode": "end_node",
            "blockingAlertUuid": "blocking_alert_uuid",
            "startNode": "start_node",
        },
        axis=1,
        inplace=True,
    )

    for key in jams_df.keys():
        if jams_df[key] is None or key not in [
            field.name for field in WazeTrafficJams.__table__.columns
        ]:
            jams_df.drop([key], axis=1, inplace=True)

    jams_df["insertion_time"] = datetime.now()
    jams_df["update_time"] = datetime.now()

    return jams_df.to_dict("records")


class ParserRetriever:
    def __init__(self):
        self.__parsers = {ALERTS: _parse_alerts, JAMS: _parse_jams}

    def get_parser(self, field: str):
        return self.__parsers.get(field, lambda rows: rows)
