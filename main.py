from flask import Flask, request, jsonify
from google.cloud import optimization_v1 as ro
from google.oauth2 import service_account
from datetime import datetime, timezone
import google.protobuf.duration_pb2 as duration_pb2
from google.cloud import bigquery
from google.protobuf.timestamp_pb2 import Timestamp
from google.cloud.optimization_v1.types import TimeWindow, Shipment
import os
import json

app = Flask(__name__)

# シークレットからサービスアカウント情報を取得
service_account_info = json.loads(os.getenv('GOOGLE_APPLICATION_CREDENTIALS_JSON'))

# 認証情報を作成
credentials = service_account.Credentials.from_service_account_info(service_account_info)

# Route Optimization APIクライアントを作成
client = ro.RouteOptimizationClient(credentials=credentials)

def makeTimeWindow(start_time_string, end_time_string):
    start_time_tm = Timestamp()
    start_time_tm.FromJsonString(start_time_string)

    end_time_tm = Timestamp()
    end_time_tm.FromJsonString(end_time_string)

    time_window = TimeWindow(
        start_time=start_time_tm,
        end_time=end_time_tm
    )

    return time_window

def getCommonParameter():
    return 360  # 秒数

def getDriverInfo():
    driver_list_data = [
        ['Driver1', '8', '0', '17', '0', '10', '1000'],
        ['Driver2', '9', '0', '18', '0', '8', '900'],
    ]
    return driver_list_data

def makeVisit(defalutDuration):
    project_id = "m2m-core"

    client_bq = bigquery.Client(credentials=credentials, project=project_id)

    query = """
         select    mmm_cleaning.id,
                      mmm_cleaning.status,
                      mmm_listing.prefecture_id,
                      mmm_listing.name as room_name,
                      mmm_okihai.building_name,
                      mmm_tour.cleaning_by,
                      mmm_latlang.latitude,
                      mmm_latlang.longitude,
                      if( mmm_okihai.okihai = "置き配不可", 1,0) as is_okihai ,--置き配不可
                      if( mmm_resv.id is null, 0,1) as out_in
            from      `m2m-core.m2m_cleaning_prod.cleaning` mmm_cleaning
            left outer join
                      `m2m-core.m2m_core_prod.listing` mmm_listing
            on
                      mmm_cleaning.listing_id = mmm_listing.id
            left outer join
                      `m2m-core.dx_001_room.room_id` mmm_room
            on
                      mmm_cleaning.listing_id = mmm_room.room_id
            left outer join
                      `m2m-core.su_wo.m2m_list_address` mmm_latlang
            on
                      mmm_cleaning.listing_id = mmm_latlang.listing_id
            left outer join
                      `m2m-core.m2m_core_prod.reservation` mmm_resv
            on
                      mmm_cleaning.cleaning_date = mmm_resv.start_date
            and       mmm_cleaning.listing_id = mmm_resv.listing_id
            and       mmm_resv.accepted = true
            left outer join
                      `m2m-core.su_wo.okihai` mmm_okihai
            on
                      mmm_room.building_id = mmm_okihai.building_id
            left outer join
                      `m2m-core.su_wo.wo_cleaning_tour` mmm_tour
            on
                      mmm_cleaning.id = mmm_tour.cleaning_id
            where
                      mmm_cleaning.cleaning_date = '2024-11-30'
            and       mmm_cleaning.photo_tour_id is null
            and       mmm_cleaning.is_disabled = false
            and       mmm_tour.cleaning_by = '自社'
            and       mmm_tour.placement_type = '部屋'
            and       mmm_listing.prefecture_id = '13'
            and       if( mmm_okihai.okihai = "置き配不可", 1,0) = 0
            and       mmm_listing.name not like 'stayme%'
            and       mmm_listing.name not like 'Elm%'
            order by  out_in desc
    """

    query_job = client_bq.query(query)

    results = query_job.result()

    result_list = []

    for row in results:
        timewindow = makeTimeWindow(
            datetime(2024, 2, 12, 22, 0, 0 , tzinfo=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
            datetime(2024, 2, 13, 12, 0, 0, tzinfo=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        )

        shipvisit = {
            "deliveries": [
                {
                    "arrival_location": { "latitude" : row.latitude, "longitude": row.longitude },
                    "duration": duration_pb2.Duration(seconds=360),
                    "label": row.room_name,
                    "load_demands": {"pallets": {"amount" :2} },
                    "time_windows": [timewindow]
                },
            ],
        }
        result_list.append(shipvisit)

    return result_list

def makeVehicle(drivers):
    v_lat   = 35.836189
    v_long  = 139.814385

    vehicles = []

    for driver in drivers:
        s_h = int(driver[1])
        s_m = int(driver[2])
        e_h = int(driver[3])
        e_m = int(driver[4])

        timewindow_st = makeTimeWindow(
            datetime(2024, 2, 13, s_h, s_m, 0 , tzinfo=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
            datetime(2024, 2, 13, s_h+1, s_m+30, 0 , tzinfo=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        )
        timewindow_en = makeTimeWindow(
            datetime(2024, 2, 13, e_h-1, e_m, 0 , tzinfo=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
            datetime(2024, 2, 13, e_h, e_m, 0 , tzinfo=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        )

        in_vehicle = {
            "display_name": driver[0],
            "label": driver[0],
            "ignore": False,
            "cost_per_hour": int(driver[6]),
            "cost_per_kilometer": int(int(driver[6]) / 3),
            "cost_per_traveled_hour": int(int(driver[6]) / 3),
            "travel_mode": "DRIVING",
            "start_location": {"latitude": v_lat, "longitude": v_long},
            "end_location": {"latitude": v_lat, "longitude": v_long},
            "start_time_windows": [timewindow_st],
            "end_time_windows": [timewindow_en],
            "load_limits": {
                "pallets": {
                    "max_load": int(driver[5])
                }
            },
            "cost_per_kilometer": 1.0
        }
        vehicles.append(in_vehicle)

    return vehicles

def process_response(response):
    tours = response.routes
    results = []

    for tour in tours:
        if tour.vehicle_start_time is None:
            continue

        driver_result = {
            "driver_id": tour.vehicle_label,
            "start_time": tour.vehicle_start_time.isoformat(),
            "end_time": tour.vehicle_end_time.isoformat(),
            "visits": [],
        }

        for visit in tour.visits:
            visit_info = {
                "visit_label": visit.visit_label,
                "arrival_time": visit.start_time.isoformat(),
            }
            driver_result["visits"].append(visit_info)

        results.append(driver_result)

    return results

@app.route('/optimize_routes', methods=['GET'])
def optimize_routes_endpoint():
    drivers_list = getDriverInfo()
    duration = getCommonParameter()

    request = ro.OptimizeToursRequest(
        parent="projects/your_project_id",
        model={
            "shipments": makeVisit(defalutDuration=duration),
            "vehicles": makeVehicle(drivers=drivers_list),
            "global_start_time": datetime(2024, 2, 12, 20, 0, 0, tzinfo=timezone.utc).isoformat(),
            "global_end_time": datetime(2024, 2, 13, 15, 0, 0, tzinfo=timezone.utc).isoformat()
        }
    )

    try:
        response = client.optimize_tours(request=request)
        results = process_response(response)
        return jsonify(results)
    except Exception as e:
        return jsonify({"error": str(e)}), 500

if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=3000)