import pandas as pd

from anyway_etl.waze.models import WazeAlert


class WazeParserRetriever:
    def __init__(self):
        self.__parsers = {"alerts": self.__parse_alert}

    def __parse_alert(self, raw_alert):
        alert_df = pd.json_normalize(raw_alert)

        alert_df["created_at"] = pd.to_datetime(alert_df["pubMillis"], unit="ms")

        alert_df.rename(
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

        alert_df["geom"] = alert_df.apply(
            lambda row: "POINT({} {})".format(row["longitude"], row["latitude"]), axis=1
        )

        alert_df["road_type"] = int(alert_df["road_type"].fillna(-1)[0])
        alert_df["number_thumbs_up"] = int(
            alert_df.get("number_thumbs_up").fillna(0)[0]
        )
        alert_df["report_by_municipality_user"] = _convert_to_bool(
            alert_df.get("report_by_municipality_user", False)
        )

        alert_df.drop(["country", "pubMillis"], axis=1, inplace=True, errors="ignore")

        for key in alert_df.keys():
            if alert_df[key] is None or key not in [
                field.name for field in WazeAlert.__table__.columns
            ]:
                alert_df.drop([key], axis=1, inplace=True)

        return alert_df.to_dict("records")[0]

    def get_parser(self, field: str):
        return self.__parsers.get(field, lambda row: row)
