import pandas as pd

from .config import CBS_INVOLVED_ROOT_PATH
from .parse_common import common_main, get_data_value
from .parse_localization import get_localization_city_name


def get_involved(stats, files_from_cbs):
    for _, involve in files_from_cbs['involved'].iterrows():
        if not involve.get("PK_TEUNA_FIKT") or pd.isnull(
            involve.get("PK_TEUNA_FIKT")
        ):  # skip lines with no accident id
            stats['skipped_lines_no_accident_id'] += 1
            continue
        stats['valid_lines'] += 1
        yield {
            "accident_id": int(involve.get("PK_TEUNA_FIKT")),
            "provider_and_id": int(
                str(int(involve.get("SUG_TIK")))
                + str(int(involve.get("PK_TEUNA_FIKT")))
            ),
            "provider_code": int(involve.get("SUG_TIK")),
            "file_type_police": get_data_value(involve.get("SUG_TIK_MISHTARA")),
            "involved_type": int(involve.get("SUG_MEORAV")),
            "license_acquiring_date": int(involve.get("SHNAT_HOZAA")),
            "age_group": int(involve.get("KVUZA_GIL")),
            "sex": get_data_value(involve.get("MIN")),
            "vehicle_type": get_data_value(involve.get("SUG_REHEV_NASA_LMS")),
            "safety_measures": get_data_value(involve.get("EMZAE_BETIHUT")),
            "involve_yishuv_symbol": get_data_value(
                involve.get("SEMEL_YISHUV_MEGURIM")
            ),
            "involve_yishuv_name": get_localization_city_name(
                involve.get("SEMEL_YISHUV_MEGURIM")
            ),
            "injury_severity": get_data_value(involve.get("HUMRAT_PGIA")),
            "injured_type": get_data_value(involve.get("SUG_NIFGA_LMS")),
            "injured_position": get_data_value(involve.get("PEULAT_NIFGA_LMS")),
            "population_type": get_data_value(involve.get("KVUTZAT_OHLUSIYA_LMS")),
            "home_region": get_data_value(involve.get("MAHOZ_MEGURIM")),
            "home_district": get_data_value(involve.get("NAFA_MEGURIM")),
            "home_natural_area": get_data_value(involve.get("EZOR_TIVI_MEGURIM")),
            "home_municipal_status": get_data_value(
                involve.get("MAAMAD_MINIZIPALI_MEGURIM")
            ),
            "home_yishuv_shape": get_data_value(involve.get("ZURAT_ISHUV_MEGURIM")),
            "hospital_time": get_data_value(involve.get("PAZUAUSHPAZ_LMS")),
            "medical_type": get_data_value(involve.get("ISS_LMS")),
            "release_dest": get_data_value(involve.get("YAADSHIHRUR_PUF_LMS")),
            "safety_measures_use": get_data_value(involve.get("SHIMUSHBEAVIZAREYBETIHUT_LMS")),
            "late_deceased": get_data_value(involve.get("PTIRAMEUHERET_LMS")),
            "car_id": get_data_value(involve.get("MISPAR_REHEV_FIKT")),
            "involve_id": get_data_value(involve.get("ZEHUT_FIKT")),
            "accident_year": get_data_value(involve.get("SHNAT_TEUNA")),
            "accident_month": get_data_value(involve.get("HODESH_TEUNA")),
        }


def main(load_start_year=None):
    common_main(load_start_year, CBS_INVOLVED_ROOT_PATH, get_involved)
