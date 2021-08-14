import pandas as pd
from anyway.models import WazeAlert


def __convert_to_bool(value):
    if isinstance(value, bool):
        return value
    else:
        return str(value).lower() in ("yes", "true", "t", "1")


def _parse_alerts(rows):
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

    alerts_df["road_type"] = int(alerts_df["road_type"].fillna(-1)[0])
    alerts_df["number_thumbs_up"] = int(alerts_df.get("number_thumbs_up", 0))
    alerts_df["report_by_municipality_user"] = __convert_to_bool(
        alerts_df.get("report_by_municipality_user", False)
    )

    alerts_df.drop(["country", "pubMillis"], axis=1, inplace=True, errors="ignore")

    for key in alerts_df.keys():
        if alerts_df[key] is None or key not in [
            field.name for field in WazeAlert.__table__.columns
        ]:
            alerts_df.drop([key], axis=1, inplace=True)

    return alerts_df.to_dict("records")


class WazeParserRetriever:
    def __init__(self):
        self.__parsers = {"alerts": _parse_alerts}

    def get_parser(self, field: str):
        return self.__parsers.get(field, lambda rows: rows)
