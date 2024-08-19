from flask import (
    Blueprint, jsonify, request, abort
)
from db import get_db
from .api_db import APIDB
from logger import logger

api_routes = Blueprint('api_routes', __name__)