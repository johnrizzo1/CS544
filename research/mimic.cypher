
// SHOW DATABASES;
// CREATE DATABASE mimic IF NOT EXISTS;
// USING PERIODIC COMMIT 500
// LOAD CSV WITH HEADERS FROM 'file:///Users/jrizzo/Projects/courses/CS544/data/mimic-iii-clinical-database-1.4/ADMISSIONS.csv' AS line
// CREATE (
//     :Admissions { 
//         row_id: toInteger(line.ROW_ID), 
//         subject_id: toInteger(line.SUBJECT_ID), 
//         hadm_id: toInteger(line.HADM_ID),
//         admittime: datetime( REPLACE(line.ADMITTIME, ' ', 'T')), 
//         dischtime: datetime( REPLACE(line.DISCHTIME, ' ', 'T')),
//         deathtime: datetime( REPLACE(line.DEATHTIME, ' ', 'T')), 
//         admission_type: line.ADMISSION_TYPE,
//         admission_location: line.ADMISSION_LOCATION, 
//         discharge_location: line.DISCHARGE_LOCATION,
//         insurance: line.INSURANCE,
//         language: line.LANGUAGE, 
//         religion: line.RELIGION,
//         marital_status: line.MARITAL_STATUS,
//         ethnicity: line.ETHNICITY, 
//         edregtime: datetime( REPLACE(line.EDREGTIME, ' ', 'T')),
//         edouttime: datetime( REPLACE(line.EDOUTTIME, ' ', 'T')), 
//         diagnosis: line.DIAGNOSIS,
//         hospital_expire_flag: toInteger(line.HOSPITAL_EXPIRE_FLAG), 
//         has_chartevents_data: toInteger(line.HAS_CHARTEVENTS_DATA),
//         neo4j_import_time: datetime()
//     }
// );
