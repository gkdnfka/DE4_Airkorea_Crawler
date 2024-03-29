import os
import json
import datetime

import requests

from utils import get_hour, convert_dt, safe_cast


def request_airkorea_api(station_name, page_no, data_term="MONTH"):
    response = requests.get(
        f"{os.environ.get('AIRKOREA_API_URL')}"
        f"?serviceKey={os.environ.get('AIRKOREA_API_KEY')}"
        f"&returnType=json"
        f"&numOfRows=100"
        f"&pageNo={page_no}"
        f"&stationName={station_name}"
        f"&dataTerm={data_term}"
    )

    return response


def parse_airdata(content):
    result = []
    airdata = json.loads(content)["response"]["body"]["items"]

    for data in airdata:
        temp_dt = data["dataTime"]
        hour = get_hour(temp_dt)

        if hour == "24":
            dt = convert_dt(temp_dt)
        else:
            dt = datetime.datetime.strptime(temp_dt, "%Y-%m-%d %H:%M")

        result.append({
            "event_time": int(dt.timestamp()),
            "pm_10": safe_cast(data["pm10Value"], int, None),
            "o3": safe_cast(data["o3Value"], float, None),
            "no2": safe_cast(data["no2Value"], float, None),
            "co": safe_cast(data["coValue"], float, None),
            "so2": safe_cast(data["so2Value"], float, None)
        })

    return result