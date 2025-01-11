# COLLECT DAY CANDLESTICKS AND VOLUME
import datetime
import os
import logging
import requests
import pandas as pd

logging.basicConfig(level=logging.INFO)

def get_date(timestamp):
    datetime_object = datetime.datetime.fromtimestamp(timestamp)
    return datetime_object.strftime("%Y/%m/%d")

def historical_ohlcv(
        instrument: str = "XRP-USD",
        interval: str = "days",
        limit: int = 2000,
        to_ts: int = None
):
    params = {
        "market": "ccix",
        "instrument": instrument,
        "limit": limit,
        "aggregate": 1,
        "fill": "true",
        "apply_mapping": "true",
        "groups":"OHLC,VOLUME",
        "response_format": "JSON",
    }
    if to_ts:
        params['to_ts'] = to_ts
    try:
        response = requests.get(
            f"https://data-api.cryptocompare.com/index/cc/v1/historical/{interval}",
            params=params,
            headers={
                "Content-type":"application/json; charset=UTF-8",
                "x-api-key": os.getenv('API_KEY')
            }
        )
        return response.json()
    except requests.exceptions.RequestException as e:
        print(e)

ten_y = 1420070400
five_y = 1577836800
three_y = 1640995200

d_path = 'xrp_usd_day.parquet'
h_path = 'xrp_usd_hour.parquet'
m_path = 'xrp_usd_min.parquet'

def fetch_all_ohlcv(file_path, instrument, interval):
    json_response = historical_ohlcv(instrument, interval)
    chunk = json_response['Data']
    prev_ts = chunk[0].get('TIMESTAMP')
    logging.info(f"-- TS = {get_date(prev_ts)}")
    df = pd.DataFrame(json_response['Data'])
    df.to_parquet(file_path, engine='pyarrow', compression='snappy')
    while True:
        json_response = historical_ohlcv(instrument, interval, to_ts=prev_ts)
        chunk = json_response['Data']
        if len(chunk) <= 0:
            break
        df = pd.DataFrame(chunk)
        dpf = pd.read_parquet(file_path)
        df = pd.concat([dpf, df])
        df.to_parquet(file_path, engine='pyarrow', compression='snappy')
        prev_ts = int(chunk[0].get('TIMESTAMP'))
        logging.info(f">>> TS = {get_date(prev_ts)}")
        cur_ts = int(chunk[-1].get('TIMESTAMP'))
        if interval == 'minutes' and prev_ts <= three_y:
            break
        if interval == 'hours' and prev_ts <= five_y:
            break
        if interval == 'days' and prev_ts <= ten_y:
            break
        if prev_ts == cur_ts:
            break

fetch_all_ohlcv(d_path, instrument="SHIB-USD", interval="days")
fetch_all_ohlcv(h_path, instrument="SHIB-USD", interval="hours")
fetch_all_ohlcv(m_path, instrument="SHIB-USD", interval="minutes")
