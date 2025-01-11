import logging
import os

from dotenv import load_dotenv

logging.basicConfig(level=logging.INFO)
load_dotenv()

API_KEY = os.getenv("CC_API_KEY")

class Collector:
    def __init__(self):
        pass