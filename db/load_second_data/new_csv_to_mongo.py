# from datetime import datetime
# from uuid import uuid4
# import pandas as pd
# from pymongo import MongoClient
# import math
#
#
# def validate_and_convert_int(value):
#     if pd.isna(value) or value == '':
#         return 0
#     try:
#         return int(float(value))
#     except (ValueError, TypeError):
#         return 0
#
#
# def get_coordinates_by_country(collection, country_name):
#     existing_event = collection.find_one(
#         {
#             "location.country": country_name,
#             "location.coordinates.coordinates.0": {"$exists": True},
#             "location.coordinates.coordinates.1": {"$exists": True}
#         },
#         {"location.coordinates": 1}
#     )
#     return existing_event['location']['coordinates'] if existing_event else None
#
#
# def merge_new_data(csv_path):
#     client = MongoClient('mongodb://localhost:27017/')
#     db = client.terror_events
#     collection = db.events
#
#     df = pd.read_csv(csv_path)
#
#     for _, row in df.iterrows():
#         try:
#             # מציאת קואורדינטות מהמסד הקיים
#             coordinates = get_coordinates_by_country(collection, row['Country'])
#
#             # המרת תאריך
#             try:
#                 date_obj = datetime.strptime(row['Date'], '%Y-%m-%d')  # התאם לפורמט התאריך בקובץ
#             except ValueError:
#                 date_obj = None
#
#             event = {
#                 "eventId": str(uuid4()),
#                 "date": date_obj,
#                 "location": {
#                     "country": row.get('Country'),
#                     "region": None,  # אם אין מידע על אזור
#                     "city": None  # אם אין מידע על עיר
#                 },
#                 "attack": {
#                     "type": row.get('Weapon'),  # או שדה מתאים אחר
#                     "target": {
#                         "type": None,  # אם אין מידע על סוג מטרה
#                         "name": None  # אם אין מידע על שם המטרה
#                     }
#                 },
#                 "group": row.get('Perpetrator'),
#                 "casualties": {
#                     "killed": validate_and_convert_int(row.get('Fatalities')),
#                     "wounded": validate_and_convert_int(row.get('Injuries'))
#                 }
#             }
#
#             # הוספת קואורדינטות אם נמצאו
#             if coordinates:
#                 event['location']['coordinates'] = coordinates
#
#             collection.insert_one(event)
#             print(f"Inserted event for {row.get('Country')} on {date_obj}")
#
#         except Exception as e:
#             print(f"Error processing row: {e}")
#             continue
#
#     client.close()
#
#
# if __name__ == "__main__":
#     csv_path = 'new_data_base.csv'
#     merge_new_data(csv_path)


from datetime import datetime
from uuid import uuid4
import pandas as pd
from pymongo import MongoClient
import math
from utils.validation import validate_and_convert_int



def get_coordinates_by_country(collection, country_name):
    existing_event = collection.find_one(
        {
            "location.country": country_name,
            "location.coordinates.coordinates.0": {"$exists": True},
            "location.coordinates.coordinates.1": {"$exists": True}
        },
        {"location.coordinates": 1}
    )
    return existing_event['location']['coordinates'] if existing_event else None


def merge_new_data(csv_path):
    client = MongoClient('mongodb://localhost:27017/')
    db = client.terror_events
    collection = db.events

    df = pd.read_csv(csv_path, encoding='latin-1')

    for _, row in df.iterrows():
        try:
            # מציאת קואורדינטות מהמסד הקיים
            coordinates = get_coordinates_by_country(collection, row['Country'])

            # המרת תאריך
            try:
                date_obj = datetime.strptime(row['Date'], '%Y-%m-%d')  # התאם לפורמט התאריך בקובץ
            except ValueError:
                date_obj = None

            event = {
                "eventId": str(uuid4()),
                "date": date_obj,
                "location": {
                    "country": row.get('Country'),
                    "region": None,  # אם אין מידע על אזור
                    "city": None  # אם אין מידע על עיר
                },
                "attack": {
                    "type": row.get('Weapon'),  # או שדה מתאים אחר
                    "target": {
                        "type": None,  # אם אין מידע על סוג מטרה
                        "name": None  # אם אין מידע על שם המטרה
                    }
                },
                "group": row.get('Perpetrator'),
                "casualties": {
                    "killed": validate_and_convert_int(row.get('Fatalities')),
                    "wounded": validate_and_convert_int(row.get('Injuries'))
                }
            }

            # הוספת קואורדינטות אם נמצאו
            if coordinates:
                event['location']['coordinates'] = coordinates

            collection.insert_one(event)
            print(f"Inserted event for {row.get('Country')} on {date_obj}")

        except Exception as e:
            print(f"Error processing row: {e}")
            continue

    client.close()


if __name__ == "__main__":
    csv_path = '../../data_source/new_data_base.csv'  # עדכן את הנתיב לקובץ החדש
    merge_new_data(csv_path)