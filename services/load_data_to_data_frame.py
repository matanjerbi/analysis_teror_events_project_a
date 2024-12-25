import pandas as pd
from pymongo import MongoClient


df = None
client = MongoClient('mongodb://localhost:27017/')
db = client.terror_events


def load_data():
    global df
    data = list(db.events.find())
    df = pd.DataFrame(data)

    if len(df) > 0:
        df['attack_type'] = df['attack'].apply(lambda x: x['type'])
        df['target_type'] = df['attack'].apply(lambda x: x['target']['type'])
        df['country'] = df['location'].apply(lambda x: x['country'])
        df['region'] = df['location'].apply(lambda x: x['region'])
        df['city'] = df['location'].apply(lambda x: x['city'])
        df['longitude'] = df['location'].apply(
            lambda x: x.get('coordinates', {}).get('coordinates', [None, None])[0] if x.get('coordinates') else None)
        df['latitude'] = df['location'].apply(
            lambda x: x.get('coordinates', {}).get('coordinates', [None, None])[1] if x.get('coordinates') else None)
        df['killed'] = df['casualties'].apply(lambda x: x['killed'])
        df['wounded'] = df['casualties'].apply(lambda x: x['wounded'])
        df['score'] = df['killed'] * 2 + df['wounded']
        df['date'] = pd.to_datetime(df['date'])

    return df
