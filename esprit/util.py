from datetime import datetime

def now():
    return datetime.now().strftime("%Y-%m-%dT%H:%M:%SZ")
