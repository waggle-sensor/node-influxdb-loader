from urllib.request import urlopen
import json
import pandas as pd

def resolve_time(t):
    try:
        return pd.to_datetime(t)
    except (TypeError, ValueError):
        pass
    return pd.to_datetime("now", utc=True) + pd.to_timedelta(t)


def timestr(t):
    return t.strftime("%Y-%m-%dT%H:%M:%S.%fZ")

q = {"start": timestr(resolve_time("-2d"))}
q["filter"] = {
    "name": "env.temperature",
}

body = json.dumps(q).encode()
with urlopen("http://localhost:8086/api/v1/query", body) as f:
    print(f.readall())