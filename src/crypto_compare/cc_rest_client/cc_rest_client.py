# COLLECT CANDLES AND VOLUME
import datetime
import os
import logging
import requests
import pandas as pd

logging.basicConfig(level=logging.DEBUG)

def human_date(timestamp):
    datetime_object = datetime.datetime.fromtimestamp(timestamp)
    return datetime_object.strftime("%Y/%m/%d_%H:%M:%S")

def historical_ohlcv(
        instrument: str = "BTC-USD",
        interval: str = "days",
        limit: int = 2000,
        to_ts: int = None
):
    params = {
        "market": "cadli",
        "instrument": instrument,
        "limit": limit,
        "aggregate": 1,
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
        logging.error(e)

def process_dataframes(chunks, instrument, interval):
    df = pd.concat(chunks, ignore_index=True)
    df['DATE'] = pd.to_datetime(df['TIMESTAMP'], unit="s")
    df.sort_values(by="DATE", inplace=True)
    df.reset_index(drop=True, inplace=True)
    since = df['DATE'].iloc[0].strftime("%Y-%m-%d")
    to = df['DATE'].iloc[-1].strftime("%Y-%m-%d")
    output_directory = "./ohlcv_data"
    os.makedirs(output_directory, exist_ok=True)
    destination_path = os.path.join(
        output_directory,
        f"ohlcv_{interval}_{instrument}_{since}_to_{to}.parquet"
    )
    df.to_parquet(destination_path)

def fetch_vwap_ohlcv(instrument, interval, dl = 1, hl = 5, ml = 100):
    prev_ts = None
    chunks = []
    n_req = 0

    while True:
        # Get the data and check for error
        json_response = historical_ohlcv(instrument, interval, to_ts=prev_ts)
        if json_response.get("Data") is None:
            logging.error("Invalid data. Stopping...")
            break

        # Get the list of objects in the response
        chunk = json_response['Data']
        if len(chunk) <= 0:
            logging.error("No data in response. Stopping...")
            break

        # Create a temporary df and store it
        df = pd.DataFrame(chunk)
        chunks.append(df)

        # Get the timestamps for the start/end of the chunk
        prev_ts = int(chunk[0]["TIMESTAMP"])
        curr_ts = int(chunk[-1]["TIMESTAMP"])

        # Exit conditions
        if interval == "minutes" and n_req > ml:
            break
        if interval == "hours" and n_req > hl:
            break
        if interval == "days" and n_req > dl:
            break
        if prev_ts == curr_ts:
            break

        logging.info(f"Processed {n_req} requests for: {interval}")
        n_req += 1

    if chunks:
        process_dataframes(chunks, instrument, interval)
    else:
        logging.error("Something went wrong with the data.")

fetch_vwap_ohlcv(instrument="BTC-USD", interval="days")
fetch_vwap_ohlcv(instrument="BTC-USD", interval="hours")
fetch_vwap_ohlcv(instrument="BTC-USD", interval="minutes")
# ndf = pd.read_parquet('')
# print(ndf.head())
