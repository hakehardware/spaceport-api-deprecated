-- EVENTS
DROP TABLE IF EXISTS events;
CREATE TABLE events (
    event_id INTEGER PRIMARY KEY AUTOINCREMENT,
    event_name TEXT NOT NULL,
    event_type TEXT NOT NULL,
    event_level TEXT NOT NULL,
    event_container_alias TEXT NOT NULL,
    event_container_id TEXT NOT NULL,
    event_container_type TEXT NOT NULL,
    event_data TEXT,
    event_datetime DATETIME NOT NULL,
    event_created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
)