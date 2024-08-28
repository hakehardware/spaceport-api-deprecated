-- Enable foreign key support
PRAGMA foreign_keys = ON;

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
);

-- CONTAINERS
DROP TABLE IF EXISTS containers;
CREATE TABLE containers (
    container_id TEXT PRIMARY KEY,
    container_type TEXT NOT NULL,
    container_alias TEXT NOT NULL,
    container_status TEXT NOT NULL,
    container_image TEXT NOT NULL,
    container_started_at TIMESTAMP NOT NULL,
    container_is_cluster INTEGER NOT NULL,
    container_nats_url TEXT,
    container_ip TEXT NOT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

DROP TRIGGER IF EXISTS set_containers_timestamp;

CREATE TRIGGER set_containers_timestamp
AFTER UPDATE ON containers
FOR EACH ROW
BEGIN
    UPDATE containers 
    SET updated_at = CURRENT_TIMESTAMP 
    WHERE container_id = OLD.container_id;
END;

-- FARMERS
DROP TABLE IF EXISTS farmers;
CREATE TABLE farmers (
    farmer_id TEXT PRIMARY KEY,
    container_id TEXT,
    farmer_reward_address TEXT,
    farmer_status INTEGER DEFAULT 1,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (container_id) REFERENCES containers(container_id) ON DELETE CASCADE
);

DROP TRIGGER IF EXISTS set_farmers_timestamp;

CREATE TRIGGER set_farmers_timestamp
AFTER UPDATE ON farmers
FOR EACH ROW
BEGIN
    UPDATE farmers 
    SET updated_at = CURRENT_TIMESTAMP 
    WHERE farmer_id = OLD.farmer_id;
END;

-- FARMS
DROP TABLE IF EXISTS farms;
CREATE TABLE farms (
    farm_index INTEGER NOT NULL,
    farmer_id TEXT,
    farm_id TEXT,
    farm_public_key TEXT,
    farm_genesis_hash TEXT,
    farm_size TEXT,
    farm_directory TEXT,
    farm_fastest_mode TEXT,
    farm_initial_plot_complete INTEGER DEFAULT 0,
    farm_plot_progress REAL DEFAULT 0,
    farm_latest_sector INTEGER DEFAULT 0,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY(farmer_id) REFERENCES farmers(farmer_id) ON DELETE CASCADE,
    PRIMARY KEY(farmer_id, farm_index)
);

DROP TRIGGER IF EXISTS set_farms_timestamp;

CREATE TRIGGER set_farms_timestamp
AFTER UPDATE ON farms
FOR EACH ROW
BEGIN
    UPDATE farms 
    SET updated_at = CURRENT_TIMESTAMP 
    WHERE farmer_id = OLD.farmer_id AND farm_index = OLD.farm_index;
END;

-- SECTORS
DROP TABLE IF EXISTS sectors;
CREATE TABLE sectors (
    sector_id INTEGER PRIMARY KEY AUTOINCREMENT,
    sector_index INTEGER, -- Sector being plotted
    public_key TEXT, -- Public key for farm
    complete INTEGER, -- 1 if complete 0 if incomplete 2 if errored out
    plot_time_seconds INTEGER DEFAULT 0, -- Difference between start and finish time in seconds
    farmer_id TEXT, -- Alias of the farmer that owns the plot
    plotter_id TEXT, -- Alias of the plotter that is creating the plot
    started_at TIMESTAMP, -- Time plot request happens
    finished_at TIMESTAMP, -- Time plot is completed
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY(farmer_id) REFERENCES farmers(farmer_id) ON DELETE CASCADE
);

DROP TRIGGER IF EXISTS set_sectors_timestamp;

CREATE TRIGGER set_sectors_timestamp
AFTER UPDATE ON sectors
FOR EACH ROW
BEGIN
    UPDATE sectors 
    SET updated_at = CURRENT_TIMESTAMP 
    WHERE sector_id = OLD.sector_id;
END;

-- VIEWS
-- CREATE VIEW farmer_container_view AS
-- SELECT
--     farmers.farmer_id,
--     farmers.container_id,
--     farmers.farmer_reward_address,
--     farmers.farmer_status,
--     containers.container_type,
--     containers.container_alias,
--     containers.container_status,
--     containers.container_image,
--     containers.container_started_at,
--     containers.container_is_cluster,
--     containers.container_nats_url,
--     containers.container_ip,
--     containers.container_created_at,
--     containers.container_updated_at
-- FROM
--     farmers
-- JOIN
--     containers ON farmers.container_id = containers.container_id;