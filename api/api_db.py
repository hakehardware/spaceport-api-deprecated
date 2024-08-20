import json

from datetime import datetime
from .db import get_db
from .logger import logger

class APIDB:
    @staticmethod
    def get_entity(entity, page, limit, filters, start, end, sort_column, sort_order):
        db = get_db()
        logger.info(f"Grabbing data for {entity} table")

        # Validation
        if entity not in ['events']:
            return {
                "message": f"Invalid entity name: {entity}", 
                'status_code': 400
            }
        
        if page <= 0 or limit <= 0:
            return {
                "message": f"Page and limit must be positive integers", 
                'status_code': 400
            }
        
        if not sort_column:
            return {
                "message": f"Sort column is required", 
                'status_code': 400
            }
        
        # Get columns from the database schema (this should be cached or validated against known columns)
        table_columns = [row['name'] for row in db.execute(f"PRAGMA table_info({entity})").fetchall()]
        if sort_column not in table_columns:
            return {
                "message": f"Invalid sort_column: {sort_column}", 
                'status_code': 400
            }

        # Build the filtering query
        filter_clauses = []
        filter_values = []
        for column, value in filters.items():
            filter_clauses.append(f"{column} = ?")
            filter_values.append(value)

        # Add time filtering for 'events' entity
        if entity == 'events':
            if start:
                filter_clauses.append("event_datetime >= ?")
                filter_values.append(start)
            if end:
                filter_clauses.append("event_datetime <= ?")
                filter_values.append(end)


        # Construct the filter query string
        filter_query = " AND ".join(filter_clauses)
        if filter_query:
            filter_query = f"WHERE {filter_query}"


        # Get the total number of rows
        total_query = f"SELECT COUNT(*) FROM {entity} {filter_query}"
        total_rows = db.execute(total_query, filter_values).fetchone()[0]

        # Determine if ordering is needed based on entity type
        order_clause = f"ORDER BY {sort_column} {sort_order}" if entity in ['events', 'consensus', 'claims', 'plots', 'rewards'] else ""

        # Get the paginated results
        offset = (page - 1) * limit
        paginated_query = f"SELECT * FROM {entity} {filter_query} {order_clause} LIMIT ? OFFSET ?"
        rows = db.execute(paginated_query, filter_values + [limit, offset]).fetchall()


        # Convert rows to dictionary
        result = [dict(row) for row in rows]

        return {
            'data': result,
            'total_rows': total_rows,
            'page': page,
            'limit': limit
        }

    @staticmethod
    def insert_event(data):

        REQUIRED_FIELDS = [
            'event_name', 'event_type', 'event_level', 'event_container_alias', 
            'event_container_id', 'event_container_type', 'event_datetime'
        ]

        # Check for missing fields
        missing_fields = [field for field in REQUIRED_FIELDS if not data.get(field)]

        if missing_fields:
            return {
                "message": f"Missing fields: {', '.join(missing_fields)}", 
                'status_code': 400
            }

        # Extract and validate event_datetime
        try:
            event_datetime = datetime.strptime(data['event_datetime'], '%Y-%m-%d %H:%M:%S')
        except ValueError:
            return {
                "message": f"Invalid datetime format: {data['event_datetime']}", 
                'status_code': 400
            }

        # Extract Data
        event_name = data.get('event_name')
        event_type = data.get('event_type')
        event_level = data.get('event_level')
        event_container_alias = data.get('event_container_alias')
        event_container_id = data.get('event_container_id')
        event_container_type = data.get('event_container_type')
        event_data = json.dumps(data.get('event_data', {}))

        db = get_db()

        # Check for duplicates
        response = db.execute(
            '''
            SELECT COUNT(*) FROM events 
            WHERE event_container_id = ? 
            AND event_data = ? 
            AND event_datetime = ?
            ''', 
            (event_container_id, event_data, event_datetime)
        ).fetchone()

        if response[0] > 0:
            return {'message': 'Event already exists', 'status_code': 200}
        
        # Insert the data into the database
        db.execute(
            '''
            INSERT INTO events (
                event_name,
                event_type,
                event_level,
                event_container_alias,
                event_container_id,
                event_container_type,
                event_data,
                event_datetime
            ) 
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ''', 
            (
                event_name,
                event_type,
                event_level,
                event_container_alias,
                event_container_id,
                event_container_type,
                event_data,
                event_datetime
            )
        )
        db.commit()

        return {'message': 'Event inserted successfully', 'status_code': 201}


        
