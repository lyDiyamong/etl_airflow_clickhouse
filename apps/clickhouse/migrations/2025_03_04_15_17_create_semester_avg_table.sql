CREATE TABLE IF NOT EXISTS class_yearly_scores (
    classId UInt32,                    -- Unique Class Identifier
    className String,                   -- Class Name (e.g., "Grade 10A")
    subjectName String,                      -- Subject Name (e.g., Math, Khmer)

    -- Dynamic Monthly Scores using Arrays    
    monthDetails Array(
        Tuple(
            monthName String,
            monthScore Decimal(5,2), -- Score AVG of the month
            monthParent String, -- Name of the semester
            monthParentId UUID
        )
    ),          
    

    -- Example: (68.5, 80, 74.25) = (Semester Average, Exam Score, Final Score)
    semesterDetails Array( 
        Tuple(
            semesterId UUID, -- evaluation parent = type = semester
            semesterPeriod String, 
            semesterStartDate Nullable(Date),
            averageScore Decimal(5,2),
            examScore Decimal(5,2),
            finalScore Decimal(5,2)
        )
    ),  

    -- Yearly Score
    yearly_score Decimal(5,2),

    -- Re-exam Score (Nullable)
    reExam_score Nullable(Decimal(5,2)),

    -- Timestamp
    created_at DateTime DEFAULT now(),
    updatedAt DateTime DEFAULT now()
) ENGINE = ReplacingMergeTree(updatedAt)
ORDER BY (classId, subjectName);
