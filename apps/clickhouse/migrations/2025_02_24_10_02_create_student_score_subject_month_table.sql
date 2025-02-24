CREATE TABLE student_month_subject_score
(
    -- School & Campus
    schoolId UUID,
    campusId UUID,

    -- Structure / Class Info
    structureRecordId UUID,
    structureRecordName      String,
    groupStructureId  UUID,
    structurePath      String,

    -- Student Info
    studentId          UUID,
    studentFirstName  String,
    studentLastName   String,
    studentFirstNameNative String,
    studentLastNameNative String,
    idCard             String,   
    -- if needed
    -- ... any other student fields you want

    -- Month Evaluation (Parent)
    monthEvaluationId UUID,
    monthName          String,
    monthStartDate    DateTime, -- from attendanceColumn.startDate
    monthEndDate      DateTime, -- from attendanceColumn.endDate

    -- Subject Evaluation (Child)
    subjectEvaluationId UUID,
    subjectName          String,
    subjectMaxScore     Float64,  -- e.g. "maxScore"
    -- ... any other subject fields like "coe", "gradingMode" if needed

    -- Score Info
    score       Float64,    -- or Decimal(5,2)
    scorerId   UUID,
    markedAt   DateTime,   -- from "markedAt"
    description String,     -- if you need the "description"

    -- Timestamps
    createdAt DateTime DEFAULT now()
)
ENGINE = MergeTree
ORDER BY (monthEvaluationId, subjectEvaluationId, studentId);
