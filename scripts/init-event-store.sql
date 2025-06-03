-- Initialize Event Store Database
-- This script creates the necessary tables and indexes for the event store

-- Create events table
CREATE TABLE IF NOT EXISTS events (
    event_id VARCHAR(36) PRIMARY KEY,
    event_type VARCHAR(100) NOT NULL,
    aggregate_id VARCHAR(100) NOT NULL,
    aggregate_type VARCHAR(50) NOT NULL,
    event_version INTEGER NOT NULL DEFAULT 1,
    timestamp TIMESTAMP WITH TIME ZONE NOT NULL,
    correlation_id VARCHAR(36),
    causation_id VARCHAR(36),
    event_metadata TEXT NOT NULL DEFAULT '{}',
    event_data TEXT NOT NULL DEFAULT '{}'
);

-- Create indexes for efficient querying
CREATE INDEX IF NOT EXISTS idx_events_aggregate_id_version 
    ON events (aggregate_id, event_version);

CREATE INDEX IF NOT EXISTS idx_events_event_type 
    ON events (event_type);

CREATE INDEX IF NOT EXISTS idx_events_timestamp 
    ON events (timestamp);

CREATE INDEX IF NOT EXISTS idx_events_correlation_id 
    ON events (correlation_id);

CREATE INDEX IF NOT EXISTS idx_events_aggregate_type 
    ON events (aggregate_type);

-- Create a composite index for common queries
CREATE INDEX IF NOT EXISTS idx_events_type_timestamp 
    ON events (event_type, timestamp);

-- Create a partial index for events with correlation_id
CREATE INDEX IF NOT EXISTS idx_events_correlation_id_not_null 
    ON events (correlation_id) 
    WHERE correlation_id IS NOT NULL;

-- Create snapshots table for aggregate state snapshots
CREATE TABLE IF NOT EXISTS snapshots (
    aggregate_id VARCHAR(100) PRIMARY KEY,
    aggregate_type VARCHAR(50) NOT NULL,
    version INTEGER NOT NULL,
    snapshot_data TEXT NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW()
);

-- Create index on snapshots
CREATE INDEX IF NOT EXISTS idx_snapshots_aggregate_type 
    ON snapshots (aggregate_type);

CREATE INDEX IF NOT EXISTS idx_snapshots_created_at 
    ON snapshots (created_at);

-- Create projections table for read models
CREATE TABLE IF NOT EXISTS projections (
    projection_name VARCHAR(100) PRIMARY KEY,
    last_processed_event_id VARCHAR(36),
    last_processed_timestamp TIMESTAMP WITH TIME ZONE,
    projection_data TEXT NOT NULL DEFAULT '{}',
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW()
);

-- Create migration statistics table
CREATE TABLE IF NOT EXISTS migration_statistics (
    migration_id VARCHAR(100) PRIMARY KEY,
    status VARCHAR(50) NOT NULL,
    started_at TIMESTAMP WITH TIME ZONE,
    completed_at TIMESTAMP WITH TIME ZONE,
    extracted_count INTEGER DEFAULT 0,
    transformed_count INTEGER DEFAULT 0,
    validated_count INTEGER DEFAULT 0,
    imported_count INTEGER DEFAULT 0,
    user_count INTEGER DEFAULT 0,
    admin_count INTEGER DEFAULT 0,
    error_count INTEGER DEFAULT 0,
    statistics_data TEXT NOT NULL DEFAULT '{}',
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW()
);

-- Create indexes on migration statistics
CREATE INDEX IF NOT EXISTS idx_migration_statistics_status 
    ON migration_statistics (status);

CREATE INDEX IF NOT EXISTS idx_migration_statistics_started_at 
    ON migration_statistics (started_at);

-- Create event processing log table
CREATE TABLE IF NOT EXISTS event_processing_log (
    id SERIAL PRIMARY KEY,
    event_id VARCHAR(36) NOT NULL,
    handler_name VARCHAR(100) NOT NULL,
    processing_status VARCHAR(50) NOT NULL,
    processing_time_ms INTEGER,
    error_message TEXT,
    processed_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW()
);

-- Create indexes on event processing log
CREATE INDEX IF NOT EXISTS idx_event_processing_log_event_id 
    ON event_processing_log (event_id);

CREATE INDEX IF NOT EXISTS idx_event_processing_log_handler_name 
    ON event_processing_log (handler_name);

CREATE INDEX IF NOT EXISTS idx_event_processing_log_status 
    ON event_processing_log (processing_status);

CREATE INDEX IF NOT EXISTS idx_event_processing_log_processed_at 
    ON event_processing_log (processed_at);

-- Create function to update updated_at timestamp
CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ language 'plpgsql';

-- Create triggers to automatically update updated_at
CREATE TRIGGER update_snapshots_updated_at 
    BEFORE UPDATE ON snapshots 
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_projections_updated_at 
    BEFORE UPDATE ON projections 
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_migration_statistics_updated_at 
    BEFORE UPDATE ON migration_statistics 
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

-- Insert initial projection entries
INSERT INTO projections (projection_name, projection_data) 
VALUES 
    ('migration_stats', '{}'),
    ('user_migrations', '{}'),
    ('admin_migrations', '{}')
ON CONFLICT (projection_name) DO NOTHING;

-- Create view for event summary
CREATE OR REPLACE VIEW event_summary AS
SELECT 
    event_type,
    aggregate_type,
    COUNT(*) as event_count,
    MIN(timestamp) as first_event,
    MAX(timestamp) as last_event
FROM events 
GROUP BY event_type, aggregate_type
ORDER BY event_count DESC;

-- Create view for migration overview
CREATE OR REPLACE VIEW migration_overview AS
SELECT 
    ms.migration_id,
    ms.status,
    ms.started_at,
    ms.completed_at,
    ms.extracted_count,
    ms.transformed_count,
    ms.validated_count,
    ms.imported_count,
    ms.user_count,
    ms.admin_count,
    ms.error_count,
    CASE 
        WHEN ms.completed_at IS NOT NULL AND ms.started_at IS NOT NULL 
        THEN EXTRACT(EPOCH FROM (ms.completed_at - ms.started_at))
        ELSE NULL 
    END as duration_seconds,
    COUNT(e.event_id) as total_events
FROM migration_statistics ms
LEFT JOIN events e ON e.aggregate_id = ms.migration_id
GROUP BY ms.migration_id, ms.status, ms.started_at, ms.completed_at, 
         ms.extracted_count, ms.transformed_count, ms.validated_count, 
         ms.imported_count, ms.user_count, ms.admin_count, ms.error_count
ORDER BY ms.started_at DESC;

-- Grant permissions (adjust as needed for your security requirements)
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA public TO event_user;
GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA public TO event_user;
GRANT SELECT ON ALL VIEWS IN SCHEMA public TO event_user;