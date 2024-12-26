from flask import Flask, jsonify, request, Blueprint
import pandas as pd
from datetime import datetime
import folium
from folium import plugins
from pymongo import MongoClient
from bson import ObjectId
import branca.colormap as cm
from services.load_data_to_data_frame import load_data


connection_queries = Blueprint('shared_targets', __name__)

# חיבור למונגו
client = MongoClient('mongodb://localhost:27017/')
db = client['terror_news']
collection = db.terror_groups

# 11. זיהוי קבוצות עם מטרות משותפות באותו אזור
@connection_queries.route('/api/shared_targets', methods=['GET'])
def get_shared_targets():
    region = request.args.get('region', 'all')

    pipeline = [
        {"$match": {"region": region}} if region != 'all' else {},
        {
            "$group": {
                "_id": {
                    "region": "$region",
                    "target": "$target_type"
                },
                "groups": {"$addToSet": "$group_name"},
                "attack_count": {"$sum": 1}
            }
        },

        {
            "$match": {
                "groups": {"$size": {"$gt": 1}}
            }
        },

        {
            "$sort": {"attack_count": -1}
        }
    ]

    results = list(collection.aggregate(pipeline))

    formatted_results = []
    for r in results:
        formatted_results.append({
            "region": r['_id']['region'],
            "target": r['_id']['target'],
            "groups": list(r['groups']),
            "attack_count": r['attack_count']
        })

    return jsonify(formatted_results)

