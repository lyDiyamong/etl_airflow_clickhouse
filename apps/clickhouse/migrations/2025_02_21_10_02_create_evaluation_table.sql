-- CREATE TABLE IF NOT EXISTS clickhouse.evaluations (
--     id UUID,
--     name String,
--     description String,
--     sort Int32,
--     maxScore Int32,
--     coe Int32,
--     gradingMode Nullable(String),
--     evaluationType Nullable(String),
--     parentId UUID,
--     schoolId UUID,
--     evaluationId UUID,
--     templateId Nullable(UUID),
--     configGroupId UUID,
--     referenceId Nullable(UUID),
--     startDate Nullable(Date),
--     endDate Nullable(Date),
--     archiveStatus Int8,
--     createdAt DateTime,
--     -- Flatten children into an array structure
--     children_ids Array(UUID),
--     children_names Array(String),
--     children_types Array(String),
--     -- Scores data (array since each child can have multiple scores per subject)
--     subject_ids Array(UUID),
--     subject_names Array(String),
--     subject_scores Array(Int32)
-- ) ENGINE = MergeTree()
-- ORDER BY (schoolId, createdAt)

CREATE TABLE IF NOT EXISTS clickhouse.evaluations
(
    evaluationId   UUID,
    schoolId       UUID,
    name           String,
    description    String,
    gradingTemplate String,  -- Could be a JSON string or a pointer to a grading rules table.
    createdAt      DateTime
) ENGINE = MergeTree()
ORDER BY (schoolId, evaluationId);

CREATE TABLE IF NOT EXISTS clickhouse.subject_score (
    score Float64,
    maxScore INTEGER,
    schoolId UUID,
    evaluationId UUID,
    subjectId UUID,
    campusId Nullable(UUID),
    configGroupId UUID,
    templateId UUID,
    groupStructureId UUID,
    structurePath Nullable(UUID)
) ENGINE = MergeTree()
ORDER BY (subjectId, evaluationId);


