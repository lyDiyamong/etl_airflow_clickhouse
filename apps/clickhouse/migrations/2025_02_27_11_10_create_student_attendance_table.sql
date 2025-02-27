CREATE TABLE IF NOT EXISTS mydatabase.student_attendance
(
    studentId UUID,
    attendanceDetails Array(Tuple(
        month String,
        present UInt16,
        absenceWithPermission UInt16,
        absenceWithoutPermission UInt16,
        totalFromPreviousMonths UInt16
    ))
)
ENGINE = MergeTree()
ORDER BY studentId;
