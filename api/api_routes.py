from flask import (
    Blueprint, jsonify, request, abort
)
from .db import get_db
from .api_db import APIDB
from .logger import logger
import traceback

api_routes = Blueprint('api_routes', __name__)

@api_routes.route('/ping', methods=['GET'])
def ping():
    return jsonify({"message": "pong"}), 200

@api_routes.route('/insert/<entity>', methods=['POST'])
def insert(entity):
    data = request.json

    insert_methods = {
        'event': APIDB.insert_event,
        'container': APIDB.insert_container,
        'farmer': APIDB.insert_farmer,
        'farm': APIDB.insert_farm,
        'incomplete_sector': APIDB.insert_incomplete_sector,
        'complete_sector': APIDB.update_complete_sector
    }

    # Retrieve the appropriate insertion method based on the entity
    execute = insert_methods.get(entity)

    if not execute:
        return jsonify({"error": f"Unknown entity: {entity}"}), 400
    
    try:
        response = execute(data)
        return jsonify({"message": response['message']}), response['status_code']

    except Exception as e:
        logger.error(f'Error in insert route: {e}')
        return jsonify(f"Internal Server Error: {str(e)}"), 500

@api_routes.route('/get/<entity>', methods=['GET'])
def get_entity(entity):
    page = request.args.get('page', 1, type=int)
    limit = request.args.get('limit', 10, type=int)
    start = request.args.get('start')
    end = request.args.get('end')
    sort_order = request.args.get('sort_order', 'DESC')
    sort_column = request.args.get('sort_column')
    filters = {key: value for key, value in request.args.items() if key not in ['page', 'limit', 'start', 'end', 'sort_order', 'sort_column']}
    
    try:
        response = APIDB.get_entity(entity, page, limit, filters, start, end, sort_column, sort_order)
        return jsonify(response), 200
    
    except Exception as e:
        logger.error(f'Error in get entity route: {e}')
        return jsonify(f"Internal Server Error: {str(e)}"), 500