import json

import fire
import dotenv
import pyarrow as pa

from s3 import parquet_to_s3
from airkorea_api import request_airkorea_api, parse_airdata
from utils import get_datalake_bucket_name, get_datalake_raw_layer_path
from kafka import send_stream


def run_extract(mode):
    """
    airkorea REST API를 활용해 대기질 정보 수집.
    batch mode: 대상 스토리지(S3)에 저장.
    streaming mode: kafka로 전송.

    :param mode: 'batch' | 'streaming'
    :return: None
    """
    dotenv.load_dotenv()
    response = request_airkorea_api(
        station_name="마포구",
        page_no=1,
        data_term="MONTH")

    print(response)

    if response.status_code != 200:
        return json.dumps(response)

    parsed_airdata = parse_airdata(response.content)

    pq = pa.Table.from_pydict({
        "event_time": [item["event_time"] for item in parsed_airdata],
        "pm_10": [item["pm_10"] for item in parsed_airdata],
        "o3": [item["o3"] for item in parsed_airdata],
        "no2": [item["no2"] for item in parsed_airdata],
        "co": [item["co"] for item in parsed_airdata],
        "so2": [item["so2"] for item in parsed_airdata]
        })

    if mode == "batch":
        bucket = get_datalake_bucket_name(
            layer="raw",
            company="de403",
            region="apnortheast2",
            account="073658113926",
            env="dev"
        )
        key = get_datalake_raw_layer_path(
            source="airkorea",
            source_region="kr",
            table="airdata",
            year=2023,
            month=8,
            day=9,
            hour=10
        )

        parquet_to_s3(pq=pq, bucket=bucket, key=f"{key}/airdata.parquet")

    elif mode == "streaming":
        send_stream(topic="stream-test", data=parsed_airdata, wait_for_seconds=10)

    else:
        raise AttributeError(f"{mode} mode is wrong. check mode value.")


if __name__ == '__main__':
    fire.Fire({
        "extract": run_extract
    })
