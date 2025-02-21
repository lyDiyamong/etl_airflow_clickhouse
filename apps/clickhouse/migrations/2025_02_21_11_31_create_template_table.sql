CREATE TABLE IF NOT EXISTS clickhouse.templates
(
    templateId UUID,
    name String,
    maxScore UInt16,
    coe Float32,
    parentId UUID,
    type String,
    description String,
    active Bool,
    totalSetting String,
    -- Foreign Key
    campusId UUID,
    schoolId UUID,
    groupStructureId UUID,
    structureRecordId UUID,
    configGroupId UUID,
    autoColumns Nullable(String),
    attendance_column Nullable(String),
    updatedAt DateTime64(3, 'UTC')
)
ENGINE = MergeTree()
PARTITION BY schoolId    
ORDER BY (templateId, updatedAt)