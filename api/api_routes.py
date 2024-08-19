from flask import (
    Blueprint, jsonify, request, abort
)
from db import get_db
from .api_db import APIDB
from logger import logger

api_routes = Blueprint('api_routes', __name__)

@api_routes.route('/hello', methods=['GET'])
def hello():
    return jsonify({"message": "Hi"}), 200