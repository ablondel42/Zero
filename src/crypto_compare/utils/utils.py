from datetime import datetime

def get_date(timestamp: int) -> str:
    return datetime.fromtimestamp(timestamp).strftime("%d/%m/%Y %H:%M:%S")
