CREATE TABLE IF NOT EXISTS clickhouse.survey_answers (
    id INTEGER,
    answerId UUID,
    otherOptionAnswer Nullable(String),
    title Nullable(String),
    createdAt DateTime64(3, 'UTC'),
    updatedAt DateTime64(3, 'UTC'),

    -- Foreign keys
    userId String,
    questionId UUID,
    subjectId UUID,
    organizationId UUID,
    categoryId Nullable(UUID),
    referenceId Nullable(UUID),


    -- User answers as Nested structure
    userAnswers_type String,
    userAnswers_answers Array(String)

) ENGINE = MergeTree()
PARTITION BY toYYYYMM(createdAt)  -- Partition by year & month
ORDER BY (id, userId, questionId);
