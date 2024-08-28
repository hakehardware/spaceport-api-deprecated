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
        if entity not in ['events', 'containers', 'farmers', 'farms', 'sectors']:
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


        if filters:
            for column, value in filters.items():
                if column in table_columns:  # Ensure the filter column exists
                    filter_clauses.append(f"{column} = ?")
                    filter_values.append(value)
                else:
                    return {
                        "message": f"Invalid filter column: {column}",
                        'status_code': 400
                    }

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
        order_clause = f"ORDER BY {sort_column} {sort_order}" if sort_column else ""

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


    @staticmethod
    def insert_container(data):
        REQUIRED_FIELDS = [
            'container_id', 'container_type', 'container_alias', 'container_status', 'container_image', 'container_started_at', 'container_is_cluster', 'container_ip'
        ]

        # Check for missing fields
        missing_fields = [field for field in REQUIRED_FIELDS if field not in data or data[field] is None]

        if missing_fields:
            return {
                "message": f"Missing fields: {', '.join(missing_fields)}", 
                'status_code': 400
            }
        
        # Extract Data
        container_id = data.get('container_id')
        container_type = data.get('container_type')
        container_alias = data.get('container_alias')
        container_status = data.get('container_status')
        container_image = data.get('container_image')
        container_started_at = data.get('container_started_at')
        container_is_cluster = data.get('container_is_cluster', None)
        container_nats_url = data.get('container_nats_url', None)
        container_ip = data.get('container_ip')

        db = get_db()
        exists = db.execute("SELECT COUNT(*) FROM containers WHERE container_id = ?", (container_id,)).fetchone()[0] > 0

        if exists:
            # Update existing row
            db.execute("""
                UPDATE containers 
                SET container_type = ?, container_alias = ?, container_status = ?, container_image = ?, container_started_at = ?, 
                    container_is_cluster = ?, container_nats_url = ?, container_ip = ?
                WHERE container_id = ?
            """, (container_type, container_alias, container_status, container_image, container_started_at, container_is_cluster, container_nats_url, container_ip, container_id))
            db.commit()
            return {"message": "Updated Container", "status_code": 200}
        
        else:
            # Create new row
            db.execute("""
                INSERT INTO containers (container_id, container_type, container_alias, container_status, container_image, 
                                        container_started_at, container_is_cluster, container_nats_url, container_ip)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (container_id, container_type, container_alias, container_status, container_image, container_started_at, container_is_cluster, container_nats_url, container_ip))
            db.commit()
            return {'message': 'Inserted Container', 'status_code': 201}

    @staticmethod
    def insert_farmer(data):
        REQUIRED_FIELDS = [
            'farmer_id', 'container_id', 'farmer_status'
        ]

        # Check for missing fields
        missing_fields = [field for field in REQUIRED_FIELDS if field not in data or data[field] is None]

        if missing_fields:
            return {
                "message": f"Missing fields: {', '.join(missing_fields)}", 
                'status_code': 400
            }
        
        farmer_id = data.get('farmer_id')
        container_id = data.get('container_id')
        farmer_status = data.get('farmer_status')
        farmer_reward_address = data.get('farmer_reward_address', None)

        db = get_db()
        exists = db.execute("SELECT COUNT(*) FROM farmers WHERE farmer_id = ?", (farmer_id,)).fetchone()[0] > 0

        if exists:
            # Update existing row
            db.execute("""
                UPDATE farmers 
                SET container_id = ?, farmer_status = ?, farmer_reward_address = ?
                WHERE farmer_id = ?
            """, (container_id, farmer_status, farmer_reward_address, farmer_id))
            db.commit()
            return {"message": "Updated Farmer", "status_code": 200}
        
        else:
            # Create new row
            db.execute("""
                INSERT INTO farmers (farmer_id, container_id, farmer_status, farmer_reward_address)
                VALUES (?, ?, ?, ?)
            """, (farmer_id, container_id, farmer_status, farmer_reward_address))
            db.commit()
            return {'message': 'Inserted Farmer', 'status_code': 201}
        
    @staticmethod
    def insert_farm(data):
        REQUIRED_FIELDS = [
            'farmer_id', 'farm_index'
        ]

        # Check for missing fields
        missing_fields = [field for field in REQUIRED_FIELDS if field not in data or data[field] is None]
        if missing_fields:
            return {
                "message": f"Missing fields: {', '.join(missing_fields)}",
                'status_code': 400
            }
        
        farmer_id = data.get('farmer_id')
        farm_index = data.get('farm_index')
        farm_id = data.get('farm_id')
        farm_public_key = data.get('farm_public_key')
        farm_genesis_hash = data.get('farm_genesis_hash')
        farm_size = data.get('farm_size')
        farm_directory = data.get('farm_directory')
        farm_fastest_mode = data.get('farm_fastest_mode')
        farm_initial_plot_complete = data.get('farm_initial_plot_complete')
        farm_plot_progress = data.get('farm_plot_progress')
        farm_latest_sector = data.get('farm_latest_sector')

        db = get_db()
        exists = db.execute(
            "SELECT COUNT(*) FROM farms WHERE farmer_id = ? AND farm_index = ?",
            (farmer_id, farm_index)
        ).fetchone()[0] > 0

        if not exists:
            # Insert a new record if the farm does not exist
            query = """
            INSERT INTO farms (
                farmer_id, farm_index, farm_id, farm_public_key, farm_genesis_hash,
                farm_size, farm_directory, farm_fastest_mode, farm_initial_plot_complete,
                farm_plot_progress, farm_latest_sector
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """
            values = [
                farmer_id,
                farm_index,
                farm_id,
                farm_public_key,
                farm_genesis_hash,
                farm_size,
                farm_directory,
                farm_fastest_mode,
                farm_initial_plot_complete,
                farm_plot_progress,
                farm_latest_sector
            ]

            db.execute(query, values)
            db.commit()
            return {'message': 'Farm inserted successfully', 'status_code': 201}
        

        else:
            # Prepare the SQL query with dynamic fields for update
            fields_to_update = []
            values = []

            if farm_id is not None:
                fields_to_update.append("farm_id = ?")
                values.append(farm_id)
            if farm_public_key is not None:
                fields_to_update.append("farm_public_key = ?")
                values.append(farm_public_key)
            if farm_genesis_hash is not None:
                fields_to_update.append("farm_genesis_hash = ?")
                values.append(farm_genesis_hash)
            if farm_size is not None:
                fields_to_update.append("farm_size = ?")
                values.append(farm_size)
            if farm_directory is not None:
                fields_to_update.append("farm_directory = ?")
                values.append(farm_directory)
            if farm_fastest_mode is not None:
                fields_to_update.append("farm_fastest_mode = ?")
                values.append(farm_fastest_mode)
            if farm_initial_plot_complete is not None:
                fields_to_update.append("farm_initial_plot_complete = ?")
                values.append(farm_initial_plot_complete)
            if farm_plot_progress is not None:
                fields_to_update.append("farm_plot_progress = ?")
                values.append(farm_plot_progress)
            if farm_latest_sector is not None:
                fields_to_update.append("farm_latest_sector = ?")
                values.append(farm_latest_sector)

            if not fields_to_update:
                return {"message": "Nothing to update", "status_code": 200}

            query = f"""
            UPDATE farms
            SET {', '.join(fields_to_update)}
            WHERE farmer_id = ? AND farm_index = ?
            """
            values.append(farmer_id)
            values.append(farm_index)

            db.execute(query, values)
            db.commit()

            return {'message': 'Farm updated successfully', 'status_code': 200}
        

    @staticmethod
    def insert_incomplete_sector(data):
        REQUIRED_FIELDS = [
            'sector_index', 'public_key', 'complete', 'plotter_id', 'event_datetime'
        ]

        # Check for missing fields
        missing_fields = [field for field in REQUIRED_FIELDS if field not in data or data[field] is None]
        if missing_fields:
            return {
                "message": f"Missing fields: {', '.join(missing_fields)}",
                'status_code': 400
            }
        
        sector_index = data.get('sector_index')
        public_key = data.get('public_key')
        complete = data.get('complete')
        plotter_id = data.get('plotter_id')
        event_datetime = data.get('event_datetime')

        db = get_db()

        exists = db.execute(
            """
            SELECT COUNT(*) FROM sectors
            WHERE sector_index = ? 
            AND public_key = ? 
            AND complete = 0
            AND created_at >= DATETIME('now', '-1 hour')
            """,
            (sector_index, public_key)
        ).fetchone()[0] > 0

        farm = db.execute(
            """
            SELECT * FROM farms WHERE farm_public_key = ?
            """, (public_key,)
        ).fetchone()

        farmer_id = "Unknown"
        
        if farm:
            farmer_id = farm['farmer_id']

        if not exists:
            query = """
            INSERT INTO sectors (
                sector_index, public_key, complete, farmer_id, plotter_id, started_at
            ) VALUES (?, ?, ?, ?, ?, ?)
            """

            values = [
                sector_index,
                public_key,
                complete,
                farmer_id,
                plotter_id,
                event_datetime
            ]

            db.execute(query, values)
            db.commit()
            return {'message': 'Sector inserted successfully', 'status_code': 201}
        
        else:
            query = """
            UPDATE sectors
            SET started_at = ?, plotter_id = ?
            WHERE sector_index = ?
            AND public_key = ?
            AND complete = ?
            AND created_at >= DATETIME('now', '-1 hour')
            """

            values = [
                event_datetime,
                plotter_id,
                sector_index,
                public_key,
                complete
            ]

            db.execute(query, values)
            db.commit()

            return {'message': 'Sector updated successfully', 'status_code': 200}
        

    @staticmethod
    def update_complete_sector(data):
        REQUIRED_FIELDS = [
            'sector_index', 'public_key', 'complete', 'plotter_id', 'event_datetime'
        ]

        # Check for missing fields
        missing_fields = [field for field in REQUIRED_FIELDS if field not in data or data[field] is None]
        if missing_fields:
            return {
                "message": f"Missing fields: {', '.join(missing_fields)}",
                'status_code': 400
            }
        
        sector_index = data.get('sector_index')
        public_key = data.get('public_key')
        complete = data.get('complete')
        plotter_id = data.get('plotter_id')
        event_datetime = data.get('event_datetime')

        db = get_db()

        farm = db.execute(
            """
            SELECT * FROM farms WHERE farm_public_key = ?
            """, (public_key,)
        ).fetchone()

        farmer_id = "Unknown"
        
        if farm:
            farmer_id = farm['farmer_id']

        sector = db.execute(
            """
            SELECT * FROM sectors
            WHERE sector_index = ? 
            AND public_key = ? 
            AND complete = 0
            AND created_at >= DATETIME('now', '-1 hour')
            """,
            (sector_index, public_key)
        ).fetchone()

        if not sector:
            query = """
            INSERT INTO sectors (
                sector_index, public_key, complete, farmer_id, plotter_id, finished_at
            ) VALUES (?, ?, ?, ?, ?, ?)
            """

            values = [
                sector_index,
                public_key,
                complete,
                farmer_id,
                plotter_id,
                event_datetime
            ]

            db.execute(query, values)
            db.commit()
            return {'message': 'Sector inserted successfully', 'status_code': 201}
        
        else:
            started_at = sector['started_at']
            logger.info(started_at)

            query = """
            UPDATE sectors
            SET 
                finished_at = ?, 
                plotter_id = ?, 
                complete = ?, 
                plot_time_seconds = (strftime('%s', ?) - strftime('%s', ?))
            WHERE 
                sector_index = ?
                AND public_key = ?
                AND complete = 0
                AND created_at >= DATETIME('now', '-1 hour')
            """

            values = [
                event_datetime,
                plotter_id,
                complete,
                event_datetime,
                started_at,
                sector_index,
                public_key,
            ]

            db.execute(query, values)
            db.commit()

            return {'message': 'Sector updated successfully', 'status_code': 200}