CREATE TABLE IF NOT EXISTS dwd_survey (
    surveyQuestionId UUID,
    surveyName String,
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
    updatedAt DateTime DEFAULT now()

) ENGINE = ReplacingMergeTree(updatedAt)
ORDER BY (surveyName)