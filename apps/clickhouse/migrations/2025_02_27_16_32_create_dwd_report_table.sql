CREATE Table IF NOT EXISTS clickhouse.dwd_report {
    eventName String,
    studentDetails 
        Tuple(
            male UInt64
            female UInt64
            other UInt64
            total UInt64
        ),
    
    teacherDetail 
        Tuple(
            male UInt64
            female UInt64
            other UInt64
            total UInt64
        ),
    eventCount UInt64
    
}