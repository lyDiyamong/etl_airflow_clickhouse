CREATE TABLE IF NOT EXISTS clickhouse.dwd_report
(
    schoolId UUID,
    schoolName String,
    eventName String,
    studentDetails Tuple(
        male UInt64,
        female UInt64,
        other UInt64,
        total UInt64
    ),
    professionalDetails Tuple(
        male UInt64,
        female UInt64,
        other UInt64,
        total UInt64
    ),
    otherDetails Tuple(
        male UInt64,
        female UInt64,
        other UInt64,
        total UInt64
    ),
    eventCount UInt64
)
ENGINE = MergeTree()
ORDER BY (schoolId, eventName)
-- INDEX idx_schoolName (schoolName) TYPE minmax GRANULARITY 1
-- INDEX idx_eventName (eventName) TYPE set(1000) GRANULARITY 1;