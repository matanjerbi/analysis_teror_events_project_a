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

