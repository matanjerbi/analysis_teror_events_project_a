from flask import Blueprint, jsonify, request
import pandas as pd
from datetime import datetime
import folium
from folium import plugins
from pymongo import MongoClient
from bson import ObjectId

crud_bp = Blueprint('crud', __name__)

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


@crud_bp.route('/events', methods=['POST'])
def create_event():
    try:
        data = request.get_json()

        doc = {
            "date": datetime.strptime(data['date'], '%Y-%m-%d'),
            "location": {
                "country": data['country'],
                "region": data['region'],
                "city": data['city'],
                "coordinates": {
                    "type": "Point",
                    "coordinates": [float(data['longitude']), float(data['latitude'])]
                } if data.get('longitude') and data.get('latitude') else None
            },
            "attack": {
                "type": data['attack_type'],
                "target": {
                    "type": data['target_type'],
                    "name": data.get('target_name', '')
                }
            },
            "group": data.get('group', 'Unknown'),
            "casualties": {
                "killed": int(data.get('killed', 0)),
                "wounded": int(data.get('wounded', 0))
            },
            "summary": data.get('summary', '')
        }

        result = db.events.insert_one(doc)

        load_data()

        return jsonify({"message": "האירוע נוצר בהצלחה", "id": str(result.inserted_id)}), 201

    except Exception as e:
        return jsonify({"error": str(e)}), 400


@crud_bp.route('/events/<event_id>', methods=['GET'])
def get_event(event_id):
    try:
        event = db.events.find_one({"_id": ObjectId(event_id)})

        if event:
            event['_id'] = str(event['_id'])
            return jsonify(event)

        return jsonify({"error": "האירוע לא נמצא"}), 404
    except Exception as e:
        return jsonify({"error": str(e)}), 400

@crud_bp.route('/events/<event_id>', methods=['PUT'])
def update_event(event_id):
    try:
        data = request.get_json()

        doc = {
            "date": datetime.strptime(data['date'], '%Y-%m-%d'),
            "location": {
                "country": data['country'],
                "region": data['region'],
                "city": data['city'],
                "coordinates": {
                    "type": "Point",
                    "coordinates": [float(data['longitude']), float(data['latitude'])]
                } if data.get('longitude') and data.get('latitude') else None
            },
            "attack": {
                "type": data['attack_type'],
                "target": {
                    "type": data['target_type'],
                    "name": data.get('target_name', '')
                }
            },
            "group": data.get('group', 'Unknown'),
            "casualties": {
                "killed": int(data.get('killed', 0)),
                "wounded": int(data.get('wounded', 0))
            },
            "summary": data.get('summary', '')
        }

        result = db.events.update_one(
            {'_id': ObjectId(event_id)},
            {'$set': doc}
        )

        if result.modified_count:
            load_data()
            return jsonify({"message": "האירוע עודכן בהצלחה"})

        return jsonify({"error": "האירוע לא נמצא"}), 404

    except Exception as e:
        return jsonify({"error": str(e)}), 400


@crud_bp.route('/events/<event_id>', methods=['DELETE'])
def delete_event(event_id):
    try:
        result = db.events.delete_one({'_id': ObjectId(event_id)})

        if result.deleted_count:
            load_data()
            return jsonify({"message": "האירוע נמחק בהצלחה"})

        return jsonify({"error": "האירוע לא נמצא"}), 404

    except Exception as e:
        return jsonify({"error": str(e)}), 400


