# # https://developers.cryptocompare.com/documentation/data-api/spot_v1_markets_instruments
# import json
# import requests
# import logging
# import os
#
# from dotenv import load_dotenv
#
# logging.basicConfig(level=logging.INFO)
# load_dotenv()
#
# class MarketDataCollector:
#     def __init__(self, instruments: list[str]):
#         self.instruments = instruments
#         self.exchanges = []
#
#     def get_exchanges(self):
#         try:
#             for instrument in self.instruments:
#                 response = requests.get(
#                     'https://data-api.cryptocompare.com/spot/v1/markets/instruments',
#                     params={
#                         "instruments": instrument,
#                         "instrument_status": "ACTIVE"
#                     },
#                     headers={
#                         "Content-type": "application/json; charset=UTF-8",
#                         "x-api-key": os.getenv('API_KEY')
#                     }
#                 )
#                 # logging.info(json.dumps(response.json(), indent=2))
#                 self.exchanges.append({
#                     instrument: list(response.json()['Data'].keys())
#                 })
#         except Exception as e:
#             logging.error(e)
#
# collector = MarketDataCollector([
#     'BTC-USD',
#     # 'ETH-USD',
#     # 'DOGE-USD',
#     # 'PEPE-USD',
#     # 'SHIB-USD'
# ])
# collector.get_exchanges()
# logging.info(json.dumps(collector.exchanges, indent=2))
#
# # TOTAL_TRADES:103110
# # TOTAL_TRADES_BUY:68803
# # TOTAL_TRADES_SELL:34307