from .config import CBS_VEHICLES_ROOT_PATH
from .parse_common import common_main, get_data_value


def get_involved(stats, files_from_cbs):
    for _, vehicle in files_from_cbs['vehicles'].iterrows():
        stats['valid_vehicles'] += 1
        yield {
            "accident_id": int(vehicle.get("PK_TEUNA_FIKT")),
            "provider_and_id": int(
                str(int(vehicle.get("SUG_TIK")))
                + str(int(vehicle.get("PK_TEUNA_FIKT")))
            ),
            "provider_code": int(vehicle.get("SUG_TIK")),
            "file_type_police": get_data_value(vehicle.get("SUG_TIK_MISHTARA")),
            "engine_volume": int(vehicle.get("NEFAH")),
            "manufacturing_year": get_data_value(vehicle.get("SHNAT_YITZUR")),
            "driving_directions": get_data_value(vehicle.get("KIVUNE_NESIA")),
            "vehicle_status": get_data_value(vehicle.get("MATZAV_REHEV")),
            "vehicle_attribution": get_data_value(vehicle.get("SHIYUH_REHEV_LMS")),
            "vehicle_type": get_data_value(vehicle.get("SUG_REHEV_LMS")),
            "seats": get_data_value(vehicle.get("MEKOMOT_YESHIVA_LMS")),
            "total_weight": get_data_value(vehicle.get("MISHKAL_KOLEL_LMS")),
            "car_id": get_data_value(vehicle.get("MISPAR_REHEV_FIKT")),
            "accident_year": get_data_value(vehicle.get("SHNAT_TEUNA")),
            "accident_month": get_data_value(vehicle.get("HODESH_TEUNA")),
            "vehicle_damage": get_data_value(vehicle.get("NEZEK")),
        }


def main(load_start_year=None):
    common_main(load_start_year, CBS_VEHICLES_ROOT_PATH, get_involved)
