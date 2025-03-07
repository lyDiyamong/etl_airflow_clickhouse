-- Migration: Create a new teacher table in the ClickHouse database
CREATE TABLE IF NOT EXISTS clickhouse.teacher_testing (
    -- Primary Key
    teacherId INTEGER,

    -- General Information
    firstName String,
    lastName String,
    firstNameNative Nullable(String),
    lastNameNative Nullable(String),
    idCard Nullable(String),
    gender Nullable(String),
    email Nullable(String),
    phone Nullable(String),
    position Nullable(String),
    createdAt DateTime,
    updatedAt DateTime DEFAULT now(),
    department Nullable(String),
    archiveStatus Int8 DEFAULT 0,

    -- Foreign keys
    schoolId UUID,
    campusId UUID,
    groupStructureId UUID,
    structureRecordId UUID,
    subjectId UUID,
    employeeId UUID

) ENGINE = ReplacingMergeTree(updatedAt)  -- Use updatedAt as version column for deduplication
PARTITION BY schoolId      -- Partition by schoolId
ORDER BY (schoolId, teacherId);   -- Primary key is schoolId and teacherId