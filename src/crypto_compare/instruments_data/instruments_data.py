# https://developers.cryptocompare.com/documentation/data-api/spot_v1_markets_instruments
import json
import requests
import logging
import os

from dotenv import load_dotenv

logging.basicConfig(level=logging.INFO)
load_dotenv()

class MarketDataCollector:
    def __init__(self):
        self.assets = []
        self.instruments = {}
        self.ticks = {}

    @staticmethod
    def get_price_in_usd(base_in_usd, quote):
        return {
            'usd': base_in_usd / quote,     # 1$ = X other
            'other': quote / base_in_usd    # 1 other = X usd
        }


    def get_instruments_on_exchanges(self):
        try:
            response = requests.get(
                'https://data-api.cryptocompare.com/spot/v1/markets/instruments',
                params={
                    "instrument_status": "ACTIVE",
                },
                headers={
                    "Content-type": "application/json; charset=UTF-8",
                    "x-api-key": os.getenv('API_KEY')
                }
            )
            exchanges = response.json().get('Data')
            for exchange, exchange_data in exchanges.items():
                instruments = exchange_data.get('instruments')
                for instrument, instrument_data in instruments.items():
                    if instrument.startswith('BTC-'):
                        # print(f"{exchange} -> {instrument}")
                        if instrument not in self.assets:
                            self.assets.append(instrument)
                        if exchange not in self.instruments:
                            self.instruments[exchange] = [instrument]
                        else:
                            self.instruments[exchange].append(instrument)
        except Exception as e:
            logging.error(e)

    def get_instruments_ticks(self, instruments):
        try:
            response = requests.get(
                'https://data-api.cryptocompare.com/index/cc/v1/latest/tick',
                params={
                    "market":"cadli",
                    "instruments": instruments,
                    "apply_mapping":"true",
                    "groups":"VALUE"
                },
                headers={
                    "Content-type": "application/json; charset=UTF-8",
                    "x-api-key": os.getenv('API_KEY')
                }
            )
            # logging.info(json.dumps(response.json(), indent=2))
            pairs = response.json().get('Data')
            for pair, pair_data in pairs.items():
                self.ticks[pair] = pair_data.get('VALUE', -1)
            return response.json()
        except Exception as e:
            logging.error(e)

collector = MarketDataCollector()
collector.get_instruments_on_exchanges()
logging.info(json.dumps(collector.instruments, indent=2))
collector.get_instruments_ticks(collector.assets)
# logging.info(json.dumps(collector.ticks, indent=2))