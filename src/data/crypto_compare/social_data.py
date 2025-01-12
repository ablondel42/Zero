import datetime
import os
import logging
import requests
import pandas as pd

logging.basicConfig(level=logging.INFO)


def get_date(timestamp):
    datetime_object = datetime.datetime.fromtimestamp(timestamp)
    return datetime_object.strftime("%Y/%m/%d")


def historical_social(
        instrument: str = "BTC",
        social: str = "code-repository",
        limit: int = 2000,
        to_ts: int = None
):
    params = {
        "asset": instrument,
        "groups": "ID,GENERAL,ACTIVITY",
        "limit": limit,
        "aggregate": 1,
        "fill": "true",
        "response_format": "JSON",
        "asset_lookup_priority": "ID",
    }
    if to_ts:
        params['to_ts'] = to_ts
    try:
        response = requests.get(
            f"https://data-api.cryptocompare.com/asset/v1/historical/{social}/days",
            params=params,
            headers={
                "Content-type": "application/json; charset=UTF-8",
                "x-api-key": os.getenv('API_KEY')
            }
        )
        return response.json()
    except requests.exceptions.RequestException as e:
        print(e)

def fetch_all_social_data(file_path, instrument, social_network):
    json_response = historical_social(instrument, social_network)
    chunk = json_response['Data']

    if len(chunk) <= 0:
        return

    df = pd.DataFrame(chunk)
    df.to_parquet(file_path, engine='pyarrow', compression='snappy')

    prev_ts = chunk[0].get('TIMESTAMP')
    first_ts = int(prev_ts)

    logging.info(f"-- TS = {get_date(prev_ts)}")

    while True:
        json_response = historical_social(instrument, social_network, to_ts=prev_ts)
        chunk = json_response['Data']

        if len(chunk) <= 0:
            return

        df = pd.DataFrame(chunk)
        dpf = pd.read_parquet(file_path)
        df = pd.concat([df, dpf])
        df.to_parquet(file_path, engine='pyarrow', compression='snappy')

        prev_ts = int(chunk[0].get('TIMESTAMP'))
        cur_ts = int(chunk[-1].get('TIMESTAMP'))

        logging.info(f">>> prev TS = {get_date(prev_ts)}")
        logging.info(f">>> curr TS = {get_date(cur_ts)}")

        if first_ts > cur_ts:
            break


assets = ['BTC', 'ETH', 'DOGE', 'PEPE', 'SHIB']
socials = ['code-repository', 'discord', 'reddit', 'telegram', 'twitter']

for asset in assets:
    for social in socials:
        path = f'{asset.lower()}_{social}.parquet'
        fetch_all_social_data(path, asset, social)





# final_df = pd.read_parquet(path)
# print(final_df.info())
# print()
# print(len(final_df.drop_duplicates()))
