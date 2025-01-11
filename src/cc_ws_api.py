import logging
import os
import time

from dotenv import load_dotenv

logging.basicConfig(level=logging.INFO)
load_dotenv()

API_KEY = os.getenv("CC_API_KEY")

import asyncio
import json
import websockets

async def ccdata():
    url = "wss://data-streamer.cryptocompare.com?api_key="
    async with websockets.connect(url) as websocket:
        await websocket.send(json.dumps({
            "action": "SUBSCRIBE",
            "type": "spot_v1_trades",
            # "groups": [
            #     "VALUE",
            #     "CURRENT_HOUR"
            # ],
            "market": "cadli",
            "instruments": [
                "BTC-USD",
                # "ETH-USD",
                # "DOGE-USD"
            ],
        }))

        while True:
            try:
                time.sleep(1)
                data = await websocket.recv()
            except websockets.ConnectionClosed:
                break
            try:
                data = json.loads(data)
                print(json.dumps(data, indent=4))
            except ValueError:
                print(data)

asyncio.get_event_loop().run_until_complete(ccdata())