from datetime import datetime
import pandas as pd
from pymongo import MongoClient, GEOSPHERE
import numpy as np
import math
from utils.validation import validate_and_convert_float


# קריאת הקובץ
df = pd.read_csv('../../data_source/big_data_teror_events.csv',
                 encoding='latin-1',
                 low_memory=False,
                 usecols=[
                     'eventid', 'iyear', 'imonth', 'iday',
                     'country_txt', 'region_txt', 'city', 'latitude', 'longitude',
                     'attacktype1_txt', 'targtype1_txt', 'target1',
                     'gname', 'nkill', 'nwound', 'summary'
                 ])

# המרה למסמכי מונגו
events = []
for _, row in df.iterrows():
    year = row['iyear']
    month = row['imonth'] if row['imonth'] != 0 else 1
    day = row['iday'] if row['iday'] != 0 else 1

    event = {
        "eventId": str(row['eventid']),
        "date": datetime(year, month, day),
        "location": {
            "country": row['country_txt'],
            "region": row['region_txt'],
            "city": row['city']
        },
        "attack": {
            "type": row['attacktype1_txt'],
            "target": {
                "type": row['targtype1_txt'],
                "name": row['target1']
            }
        },
        "group": row['gname'],
        "casualties": {
            "killed": int(row['nkill']) if pd.notna(row['nkill']) else 0,
            "wounded": int(row['nwound']) if pd.notna(row['nwound']) else 0
        },
        "summary": row['summary'] if pd.notna(row['summary']) else None
    }

    # בדיקת קואורדינטות תקינות
    longitude = validate_and_convert_float(row['longitude'])
    latitude = validate_and_convert_float(row['latitude'])

    if longitude is not None and latitude is not None:
        # בדיקה שהקואורדינטות הן בטווח תקין
        if -180 <= longitude <= 180 and -90 <= latitude <= 90:
            event['location']['coordinates'] = {
                "type": "Point",
                "coordinates": [longitude, latitude]
            }

    events.append(event)

try:
    client = MongoClient('mongodb://localhost:27017/')
    db = client['terror_events']

    # מחיקת האוסף הקיים
    db.events.drop()

    # יצירת אינדקס גיאוגרפי עם sparse=True
    db.events.create_index([("location.coordinates", GEOSPHERE)], sparse=True)

    # הכנסת המסמכים בקבוצות
    batch_size = 1000
    total_inserted = 0

    for i in range(0, len(events), batch_size):
        batch = events[i:i + batch_size]
        try:
            result = db.events.insert_many(batch)
            total_inserted += len(result.inserted_ids)
            print(f"Inserted events {i} to {i + len(batch)} (Total: {total_inserted})")
        except Exception as e:
            print(f"Error in batch {i} to {i + len(batch)}: {str(e)}")

    print(f"Data import completed. Total documents inserted: {total_inserted}")

except Exception as e:
    print(f"An error occurred: {str(e)}")

finally:
    if 'client' in locals():
        client.close()