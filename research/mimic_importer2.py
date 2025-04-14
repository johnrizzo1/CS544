import os
from py2neo import Graph, Node, Relationship
import pandas as pd
from tqdm import tqdm  # For progress bars

import psycopg
from sqlalchemy import create_engine

from dotenv import load_dotenv
load_dotenv()

# Step 1: Connect to the Neo4j database
graph = Graph(os.environ["NEO4J_URI"], auth=(os.environ["NEO4J_USER"], os.environ["NEO4J_PASSWORD"]))
CSV_PATH = "data/mimic-iii-clinical-database-1.4"
# URI = f'postgresql+psycopg://{os.environ["DBUSER"]}:{os.environ["DBPASS"]}@{os.environ["DBHOST"]}:{os.environ["DBPORT"]}/{os.environ["DBNAME"]}'
URI = f'postgresql+psycopg://jrizzo:wh4t3fr!@irl:5432/mimic'

TABLES = {
    "ADMISSIONS": "Admission",
    "CALLOUT": "Callout",
    "CAREGIVERS": "Caregiver",
    "CPTEVENTS": "CptEvent",
    "D_CPT": "Dcpt",
    "D_ICD_DIAGNOSES": "DicdDiagnosis",
    "D_ICD_PROCEDURES": "DicdProcedure",
    "D_ITEMS": "Ditem",
    "D_LABITEMS": "Dlabitem",
    "DATETIMEEVENTS": "DatetimeEvent",
    "DIAGNOSES_ICD": "DiagnosisIcd",
    "DRGCODES": "Drgcode",
    "ICUSTAYS": "Icustay",
    "INPUTEVENTS_CV": "InputEventCv",
    "INPUTEVENTS_MV": "InputEventMv",
    "LABEVENTS": "LabEvent",
    "MICROBIOLOGYEVENTS": "MicrobiologyEvent",
    "NOTEEVENTS": "NoteEvent",
    "OUTPUTEVENTS": "OutputEvent",
    "PATIENTS": "Patient",
    "PRESCRIPTIONS": "Prescription",
    "PROCEDUREEVENTS_MV": "ProcedureEventMv",
    "PROCEDURES_ICD": "ProcedureIcd",
    "SERVICES": "Service",
    "TRANSFERS": "Transfer",
}

# Step 2: Define the schema
def create_schema():
    """
    Create constraints and indexes for the Neo4j database schema.
    """
    print("Creating schema...")
    # Drop constraints / indices
    for v in TABLES.values():
        graph.run(f"CREATE CONSTRAINT IF NOT EXISTS ON (x:{v}) ASSERT x.row_id IS UNIQUE;")
    print("Schema created successfully!")

# Step 3: Load and preprocess data
def load_data():
    """
    Load data from CSV files into Pandas DataFrames.
    Replace 'path_to_csv' with the actual path to your CSV files.
    """
    print("Loading data...")
    # for chunk in pd.read_csv(f"{CSV_PATH}/ADMISSIONS.csv", chunksize=10000):
    #     for _, row in tqdm(chunk.iterrows(), total=chunk.shape[0], desc="Admissions"):
    #         admission_node = Node(
    #             "Admission",
    #             row_id=row["ROW_ID"],
    #             subject_id=row["SUBJECT_ID"],
    #             hadm_id=row["HADM_ID"],
    #             admittime=row["ADMITTIME"],
    #             dischtime=row["DISCHTIME"],
    #             deathtime=row["DEATHTIME"],
    #             admission_type=row["ADMISSION_TYPE"],
    #             admission_location=row["ADMISSION_LOCATION"],
    #             discharge_location=row["DISCHARGE_LOCATION"],
    #             insurance=row["INSURANCE"],
    #             language=row["LANGUAGE"],
    #             religion=row["RELIGION"],
    #             marital_status=row["MARITAL_STATUS"],
    #             ethnicity=row["ETHNICITY"],
    #             edregtime=row["EDREGTIME"],
    #             edouttime=row["EDOUTTIME"],
    #             diagnosis=row["DIAGNOSIS"],
    #             hospital_expire_flag=row["HOSPITAL_EXPIRE_FLAG"],
    #             has_chartevents_data=row["HAS_CHARTEVENTS_DATA"]
    #         )
    #         graph.merge(admission_node, "Admission", "row_id")

    # for chunk in pd.read_csv(f"{CSV_PATH}/CALLOUT.csv", chunksize=10000):
    #     for _, row in tqdm(chunk.iterrows(), total=chunk.shape[0], desc="Callout"):
    #         callout_node = Node(
    #             "Callout",
    #             row_id=row["ROW_ID"],
    #             subject_id=row["SUBJECT_ID"],
    #             hadm_id=row["HADM_ID"],
    #             submit_wardid=row["SUBMIT_WARDID"],
    #             submit_careunit=row["SUBMIT_CAREUNIT"],
    #             curr_wardid=row["CURR_WARDID"],
    #             curr_careunit=row["CURR_CAREUNIT"],
    #             callout_wardid=row["CALLOUT_WARDID"],
    #             callout_service=row["CALLOUT_SERVICE"],
    #             request_tele=row["REQUEST_TELE"],
    #             request_resp=row["REQUEST_RESP"],
    #             request_cdiff=row["REQUEST_CDIFF"],
    #             request_mrsa=row["REQUEST_MRSA"],
    #             request_vre=row["REQUEST_VRE"],
    #             callout_status=row["CALLOUT_STATUS"],
    #             callout_outcome=row["CALLOUT_OUTCOME"],
    #             discharge_wardid=row["DISCHARGE_WARDID"],
    #             acknowledge_status=row["ACKNOWLEDGE_STATUS"],
    #             createtime=row["CREATETIME"],
    #             updatetime=row["UPDATETIME"],
    #             acknowledgetime=row["ACKNOWLEDGETIME"],
    #             outcometime=row["OUTCOMETIME"],
    #             firstreservationtime=row["FIRSTRESERVATIONTIME"],
    #             currentreservationtime=row["CURRENTRESERVATIONTIME"],
    #         )
    #         graph.merge(callout_node, "Callout", "row_id")
    
    # for chunk in pd.read_csv(f"{CSV_PATH}/CAREGIVERS.csv", chunksize=10000):
    #     for _, row in tqdm(chunk.iterrows(), total=chunk.shape[0], desc="Caregivers"):
    #         caregiver_node = Node(
    #             "Caregiver",
    #             row_id=row["ROW_ID"],
    #             cgid=row["CGID"],
    #             label=row["LABEL"],
    #             description=row["DESCRIPTION"]
    #         )
    #         graph.merge(caregiver_node, "Caregiver", "row_id")

    # for chunk in pd.read_csv(f"{CSV_PATH}/CHARTEVENTS.csv", chunksize=10000):
    #     print("Inserting chunk...")
    #     # print(chunk.head())
    #     for _, row in tqdm(chunk.iterrows(), total=chunk.shape[0], desc="ChartEvents"):
    #         chartevent_node = Node(
    #             "ChartEvent",
    #             row_id=row["ROW_ID"],
    #             subject_id=row["SUBJECT_ID"],
    #             hadm_id=row["HADM_ID"],
    #             icustay_id=row["ICUSTAY_ID"],
    #             itemid=row["ITEMID"],
    #             charttime=row["CHARTTIME"],
    #             storetime=row["STORETIME"],
    #             cgid=row["CGID"],
    #             value=row["VALUE"],
    #             valuenum=row["VALUENUM"],
    #             valueuom=row["VALUEUOM"],
    #             warning=row["WARNING"],
    #             error=row["ERROR"],
    #             resultstatus=row["RESULTSTATUS"],
    #             stopped=row["STOPPED"]
    #         )
    #         graph.merge(chartevent_node, "ChartEvent", "row_id")  # Use merge to avoid duplicates

    # for chunk in pd.read_csv(f"{CSV_PATH}/CPTEVENTS.csv", chunksize=10000):
    #     for _, row in tqdm(chunk.iterrows(), total=chunk.shape[0], desc="CptEvents"):
    #         cptevent_node = Node(
    #             "CptEvent",
    #             row_id=row["ROW_ID"],
    #             subject_id=row["SUBJECT_ID"],
    #             hadm_id=row["HADM_ID"],
    #             costcenter=row["COSTCENTER"],
    #             chartdate=row["CHARTDATE"],
    #             cpt_cd=row["CPT_CD"],
    #             cpt_number=row["CPT_NUMBER"],
    #             cpt_suffix=row["CPT_SUFFIX"],
    #             ticket_id_seq=row["TICKET_ID_SEQ"],
    #             sectionheader=row["SECTIONHEADER"],
    #             subsectionheader=row["SUBSECTIONHEADER"],
    #             description=row["DESCRIPTION"]
    #         )
    #         graph.merge(cptevent_node, "CptEvent", "row_id")

    for chunk in pd.read_csv(f"{CSV_PATH}/D_CPT.csv", chunksize=10000):
        for _, row in tqdm(chunk.iterrows(), total=chunk.shape[0], desc="Dcpt"):
            dcpt_node = Node(
                "Dcpt",
                row_id=row["ROW_ID"],
                category=row["CATEGORY"],
                sectionrange=row["SECTIONRANGE"],
                sectionheader=row["SECTIONHEADER"],
                subsectionrange=row["SUBSECTIONRANGE"],
                subsectionheader=row["SUBSECTIONHEADER"],
                codesuffix=row["CODESUFFIX"],
                mincodeinsubsection=row["MINCODEINSUBSECTION"],
                maxcodeinsubsection=row["MAXCODEINSUBSECTION"],
            )
            graph.merge(dcpt_node, "Dcpt", "row_id")
    
    for chunk in pd.read_csv(f"{CSV_PATH}/D_ICD_DIAGNOSES.csv", chunksize=10000):
        for _, row in tqdm(chunk.iterrows(), total=chunk.shape[0], desc="DicdDiagnosis"):
            dicd_diagnosis_node = Node(
                "DicdDiagnosis",
                row_id=row["ROW_ID"],
                icd9_code=row["ICD9_CODE"],
                short_title=row["SHORT_TITLE"],
                long_title=row["LONG_TITLE"]
            )
            graph.merge(dicd_diagnosis_node, "DicdDiagnosis", "row_id")
    
    for chunk in pd.read_csv(f"{CSV_PATH}/D_ICD_PROCEDURES.csv", chunksize=10000):
        for _, row in tqdm(chunk.iterrows(), total=chunk.shape[0], desc="DicdProcedure"):
            dicd_procedure_node = Node(
                "DicdProcedure",
                row_id=row["ROW_ID"],
                icd9_code=row["ICD9_CODE"],
                short_title=row["SHORT_TITLE"],
                long_title=row["LONG_TITLE"]
            )
            graph.merge(dicd_procedure_node, "DicdProcedure", "row_id")
            
    for chunk in pd.read_csv(f"{CSV_PATH}/D_ITEMS.csv", chunksize=10000):
        for _, row in tqdm(chunk.iterrows(), total=chunk.shape[0], desc="Ditem"):
            ditem_node = Node(
                "Ditem",
                row_id=row["ROW_ID"],
                itemid=row["ITEMID"],
                label=row["LABEL"],
                abbreviation=row["ABBREVIATION"],
                dbsource=row["DBSOURCE"],
                linksto=row["LINKSTO"],
                category=row["CATEGORY"],
                unitname=row["UNITNAME"],
                param_type=row["PARAM_TYPE"],
                conceptid=row["CONCEPTID"]
            )
            graph.merge(ditem_node, "Ditem", "row_id")
            
    for chunk in pd.read_csv(f"{CSV_PATH}/D_LABITEMS.csv", chunksize=10000):
        for _, row in tqdm(chunk.iterrows(), total=chunk.shape[0], desc="Dlabitem"):
            dlabitem_node = Node(
                "Dlabitem",
                row_id=row["ROW_ID"],
                itemid=row["ITEMID"],
                label=row["LABEL"],
                fluid=row["FLUID"],
                category=row["CATEGORY"],
                loinc_code=row["LOINC_CODE"]
            )
            graph.merge(dlabitem_node, "Dlabitem", "row_id")
    
    for chunk in pd.read_csv(f"{CSV_PATH}/DATETIMEEVENTS.csv", chunksize=10000):
        for _, row in tqdm(chunk.iterrows(), total=chunk.shape[0], desc="DatetimeEvent"):
            datetimeevent_node = Node(
                "DatetimeEvent",
                row_id=row["ROW_ID"],
                subject_id=row["SUBJECT_ID"],
                hadm_id=row["HADM_ID"],
                icustay_id=row["ICUSTAY_ID"],
                itemid=row["ITEMID"],
                charttime=row["CHARTTIME"],
                storetime=row["STORETIME"],
                cgid=row["CGID"],
                value=row["VALUE"],
                valueuom=row["VALUEUOM"],
                warning=row["WARNING"],
                error=row["ERROR"],
                resultstatus=row["RESULTSTATUS"],
                stopped=row["STOPPED"]
            )
            graph.merge(datetimeevent_node, "DatetimeEvent", "row_id")
            
    for chunk in pd.read_csv(f"{CSV_PATH}/DIAGNOSES_ICD.csv", chunksize=10000):
        for _, row in tqdm(chunk.iterrows(), total=chunk.shape[0], desc="DiagnosisIcd"):
            diagnosisicd_node = Node(
                "DiagnosisIcd",
                row_id=row["ROW_ID"],
                subject_id=row["SUBJECT_ID"],
                hadm_id=row["HADM_ID"],
                seq_num=row["SEQ_NUM"],
                icd9_code=row["ICD9_CODE"]
            )
            graph.merge(diagnosisicd_node, "DiagnosisIcd", "row_id")
    
    for chunk in pd.read_csv(f"{CSV_PATH}/DRGCODES.csv", chunksize=10000):
        for _, row in tqdm(chunk.iterrows(), total=chunk.shape[0], desc="Drgcode"):
            drgcode_node = Node(
                "Drgcode",
                row_id=row["ROW_ID"],
                subject_id=row["SUBJECT_ID"],
                hadm_id=row["HADM_ID"],
                drg_type=row["DRG_TYPE"],
                drg_code=row["DRG_CODE"],
                description=row["DESCRIPTION"],
                drg_severity=row["DRG_SEVERITY"],
                drg_mortality=row["DRG_MORTALITY"]
            )
            graph.merge(drgcode_node, "Drgcode", "row_id")
            
            
    for chunk in pd.read_csv(f"{CSV_PATH}/ICUSTAYS.csv", chunksize=10000):
        for _, row in tqdm(chunk.iterrows(), total=chunk.shape[0], desc="Icustay"):
            icustay_node = Node(
                "Icustay",
                row_id=row["ROW_ID"],
                subject_id=row["SUBJECT_ID"],
                hadm_id=row["HADM_ID"],
                icustay_id=row["ICUSTAY_ID"],
                dbsource=row["DBSOURCE"],
                first_careunit=row["FIRST_CAREUNIT"],
                last_careunit=row["LAST_CAREUNIT"],
                first_wardid=row["FIRST_WARDID"],
                last_wardid=row["LAST_WARDID"],
                intime=row["INTIME"],
                outtime=row["OUTTIME"],
                los=row["LOS"]
            )
            graph.merge(icustay_node, "Icustay", "row_id")
            
    for chunk in pd.read_csv(f"{CSV_PATH}/INPUTEVENTS_CV.csv", chunksize=10000):
        for _, row in tqdm(chunk.iterrows(), total=chunk.shape[0], desc="InputEventCv"):
            inputeventcv_node = Node(
                "InputEventCv",
                row_id=row["ROW_ID"],
                subject_id=row["SUBJECT_ID"],
                hadm_id=row["HADM_ID"],
                icustay_id=row["ICUSTAY_ID"],
                charttime=row["CHARTTIME"],
                itemid=row["ITEMID"],
                amount=row["AMOUNT"],
                amountuom=row["AMOUNTUOM"],
                rate=row["RATE"],
                rateuom=row["RATEUOM"],
                storetime=row["STORETIME"],
                cgid=row["CGID"],
                orderid=row["ORDERID"],
                linkorderid=row["LINKORDERID"],
                stopped=row["STOPPED"],
                newbottle=row["NEWBOTTLE"],
                originalamount=row["ORIGINALAMOUNT"],
                originalamountuom=row["ORIGINALAMOUNTUOM"],
                originalroute=row["ORIGINALROUTE"],
                originalrate=row["ORIGINALRATE"],
                originalrateuom=row["ORIGINALRATEUOM"],
                originalsite=row["ORIGINALSITE"]
            )
            graph.merge(inputeventcv_node, "InputEventCv", "row_id")

    for chunk in pd.read_csv(f"{CSV_PATH}/INPUTEVENTS_MV.csv", chunksize=10000):
        for _, row in tqdm(chunk.iterrows(), total=chunk.shape[0], desc="InputEventMv"):
            inputeventmv_node = Node(
                "InputEventMv",
                row_id=row["ROW_ID"],
                subject_id=row["SUBJECT_ID"],
                hadm_id=row["HADM_ID"],
                icustay_id=row["ICUSTAY_ID"],
                starttime=row["STARTTIME"],
                endtime=row["ENDTIME"],
                itemid=row["ITEMID"],
                amount=row["AMOUNT"],
                amountuom=row["AMOUNTUOM"],
                rate=row["RATE"],
                rateuom=row["RATEUOM"],
                storetime=row["STORETIME"],
                cgid=row["CGID"],
                orderid=row["ORDERID"],
                linkorderid=row["LINKORDERID"],
                ordercategoryname=row["ORDERCATEGORYNAME"],
                secondaryordercategoryname=row["SECONDARYORDERCATEGORYNAME"],
                ordercomponenttypedescription=row["ORDERCOMPONENTTYPEDESCRIPTION"],
                ordercategorydescription=row["ORDERCATEGORYDESCRIPTION"],
                patientweight=row["PATIENTWEIGHT"],
                totalamount=row["TOTALAMOUNT"],
                totalamountuom=row["TOTALAMOUNTUOM"],
                isopenbag=row["ISOPENBAG"],
                continueinnextdept=row["CONTINUEINNEXTDEPT"],
                cancelreason=row["CANCELREASON"],
                statusdescription=row["STATUSDESCRIPTION"],
                comments_editedby=row["COMMENTS_EDITEDBY"],
                comments_canceledby=row["COMMENTS_CANCELEDBY"],
                comments_date=row["COMMENTS_DATE"],
                originalamount=row["ORIGINALAMOUNT"],
                originalrate=row["ORIGINALRATE"]
            )
            graph.merge(inputeventmv_node, "InputEventMv", "row_id")

    for chunk in pd.read_csv(f"{CSV_PATH}/LABEVENTS.csv", chunksize=10000):
        for _, row in tqdm(chunk.iterrows(), total=chunk.shape[0], desc="LabEvent"):
            labevent_node = Node(
                "LabEvent",
                row_id=row["ROW_ID"],
                subject_id=row["SUBJECT_ID"],
                hadm_id=row["HADM_ID"],
                itemid=row["ITEMID"],
                charttime=row["CHARTTIME"],
                value=row["VALUE"],
                valuenum=row["VALUENUM"],
                valueuom=row["VALUEUOM"],
                flag=row["FLAG"]
            )
            graph.merge(labevent_node, "LabEvent", "row_id")
            
    for chunk in pd.read_csv(f"{CSV_PATH}/MICROBIOLOGYEVENTS.csv", chunksize=10000):
        for _, row in tqdm(chunk.iterrows(), total=chunk.shape[0], desc="MicrobiologyEvent"):
            microbiologyevent_node = Node(
                "MicrobiologyEvent",
                row_id=row["ROW_ID"],
                subject_id=row["SUBJECT_ID"],
                hadm_id=row["HADM_ID"],
                chartdate=row["CHARTDATE"],
                charttime=row["CHARTTIME"],
                spec_itemid=row["SPEC_ITEMID"],
                spec_type_desc=row["SPEC_TYPE_DESC"],
                org_itemid=row["ORG_ITEMID"],
                org_name=row["ORG_NAME"],
                isolate_num=row["ISOLATE_NUM"],
                ab_itemid=row["AB_ITEMID"],
                ab_name=row["AB_NAME"],
                dilution_text=row["DILUTION_TEXT"],
                dilution_comparison=row["DILUTION_COMPARISON"],
                dilution_value=row["DILUTION_VALUE"],
                interpretation=row["INTERPRETATION"]
            )
            graph.merge(microbiologyevent_node, "MicrobiologyEvent", "row_id")
    
    for chunk in pd.read_csv(f"{CSV_PATH}/NOTEEVENTS.csv", chunksize=10000):
        for _, row in tqdm(chunk.iterrows(), total=chunk.shape[0], desc="NoteEvent"):
            noteevent_node = Node(
                "NoteEvent",
                row_id=row["ROW_ID"],
                subject_id=row["SUBJECT_ID"],
                hadm_id=row["HADM_ID"],
                chartdate=row["CHARTDATE"],
                charttime=row["CHARTTIME"],
                storetime=row["STORETIME"],
                category=row["CATEGORY"],
                description=row["DESCRIPTION"],
                cgid=row["CGID"],
                iserror=row["ISERROR"],
                text=row["TEXT"]
            )
            graph.merge(noteevent_node, "NoteEvent", "row_id")
            
    for chunk in pd.read_csv(f"{CSV_PATH}/OUTPUTEVENTS.csv", chunksize=10000):
        for _, row in tqdm(chunk.iterrows(), total=chunk.shape[0], desc="OutputEvent"):
            outputevent_node = Node(
                "OutputEvent",
                row_id=row["ROW_ID"],
                subject_id=row["SUBJECT_ID"],
                hadm_id=row["HADM_ID"],
                icustay_id=row["ICUSTAY_ID"],
                charttime=row["CHARTTIME"],
                itemid=row["ITEMID"],
                value=row["VALUE"],
                valueuom=row["VALUEUOM"],
                storetime=row["STORETIME"],
                cgid=row["CGID"],
                stopped=row["STOPPED"],
                newbottle=row["NEWBOTTLE"],
                iserror=row["ISERROR"]
            )
            graph.merge(outputevent_node, "OutputEvent", "row_id")

    for chunk in pd.read_csv(f"{CSV_PATH}/PATIENTS.csv", chunksize=10000):
        for _, row in tqdm(chunk.iterrows(), total=chunk.shape[0], desc="Patient"):
            patient_node = Node(
                "Patient",
                row_id=row["ROW_ID"],
                subject_id=row["SUBJECT_ID"],
                gender=row["GENDER"],
                dob=row["DOB"],
                dod=row["DOD"],
                dod_hosp=row["DOD_HOSP"],
                dod_ssn=row["DOD_SSN"],
                expire_flag=row["EXPIRE_FLAG"]
            )
            graph.merge(patient_node, "Patient", "row_id")

    for chunk in pd.read_csv(f"{CSV_PATH}/PRESCRIPTIONS.csv", chunksize=10000):
        for _, row in tqdm(chunk.iterrows(), total=chunk.shape[0], desc="Prescription"):
            prescription_node = Node(
                "Prescription",
                row_id=row["ROW_ID"],
                subject_id=row["SUBJECT_ID"],
                hadm_id=row["HADM_ID"],
                icustay_id=row["ICUSTAY_ID"],
                startdate=row["STARTDATE"],
                enddate=row["ENDDATE"],
                drug_type=row["DRUG_TYPE"],
                drug=row["DRUG"],
                drug_name_poe=row["DRUG_NAME_POE"],
                drug_name_generic=row["DRUG_NAME_GENERIC"],
                formulary_drug_cd=row["FORMULARY_DRUG_CD"],
                gsn=row["GSN"],
                ndc=row["NDC"],
                prod_strength=row["PROD_STRENGTH"],
                dose_val_rx=row["DOSE_VAL_RX"],
                dose_unit_rx=row["DOSE_UNIT_RX"],
                form_val_disp=row["FORM_VAL_DISP"],
                form_unit_disp=row["FORM_UNIT_DISP"],
                route=row["ROUTE"]
            )
            graph.merge(prescription_node, "Prescription", "row_id")

    for chunk in pd.read_csv(f"{CSV_PATH}/PROCEDUREEVENTS_MV.csv", chunksize=10000):
        for _, row in tqdm(chunk.iterrows(), total=chunk.shape[0], desc="ProcedureEventMv"):
            procedureeventmv_node = Node(
                "ProcedureEventMv",
                row_id=row["ROW_ID"],
                subject_id=row["SUBJECT_ID"],
                hadm_id=row["HADM_ID"],
                icustay_id=row["ICUSTAY_ID"],
                starttime=row["STARTTIME"],
                endtime=row["ENDTIME"],
                itemid=row["ITEMID"],
                value=row["VALUE"],
                valueuom=row["VALUEUOM"],
                location=row["LOCATION"],
                locationcategory=row["LOCATIONCATEGORY"],
                storetime=row["STORETIME"],
                cgid=row["CGID"],
                orderid=row["ORDERID"],
                linkorderid=row["LINKORDERID"],
                ordercategoryname=row["ORDERCATEGORYNAME"],
                secondaryordercategoryname=row["SECONDARYORDERCATEGORYNAME"],
                ordercategorydescription=row["ORDERCATEGORYDESCRIPTION"],
                isopenbag=row["ISOPENBAG"],
                continueinnextdept=row["CONTINUEINNEXTDEPT"],
                cancelreason=row["CANCELREASON"],
                statusdescription=row["STATUSDESCRIPTION"],
                comments_editedby=row["COMMENTS_EDITEDBY"],
                comments_canceledby=row["COMMENTS_CANCELEDBY"],
                comments_date=row["COMMENTS_DATE"]
            )
            graph.merge(procedureeventmv_node, "ProcedureEventMv", "row_id")

    for chunk in pd.read_csv(f"{CSV_PATH}/PROCEDURES_ICD.csv", chunksize=10000):
        for _, row in tqdm(chunk.iterrows(), total=chunk.shape[0], desc="ProcedureIcd"):
            procedureicd_node = Node(
                "ProcedureIcd",
                row_id=row["ROW_ID"],
                subject_id=row["SUBJECT_ID"],
                hadm_id=row["HADM_ID"],
                seq_num=row["SEQ_NUM"],
                icd9_code=row["ICD9_CODE"]
            )
            graph.merge(procedureicd_node, "ProcedureIcd", "row_id")
            
    for chunk in pd.read_csv(f"{CSV_PATH}/SERVICES.csv", chunksize=10000):
        for _, row in tqdm(chunk.iterrows(), total=chunk.shape[0], desc="Service"):
            service_node = Node(
                "Service",
                row_id=row["ROW_ID"],
                subject_id=row["SUBJECT_ID"],
                hadm_id=row["HADM_ID"],
                transfertime=row["TRANSFERTIME"],
                prev_service=row["PREV_SERVICE"],
                curr_service=row["CURR_SERVICE"]
            )
            graph.merge(service_node, "Service", "row_id")
            
    for chunk in pd.read_csv(f"{CSV_PATH}/TRANSFERS.csv", chunksize=10000):
        for _, row in tqdm(chunk.iterrows(), total=chunk.shape[0], desc="Transfer"):
            transfer_node = Node(
                "Transfer",
                row_id=row["ROW_ID"],
                subject_id=row["SUBJECT_ID"],
                hadm_id=row["HADM_ID"],
                icustay_id=row["ICUSTAY_ID"],
                dbsource=row["DBSOURCE"],
                eventtype=row["EVENTTYPE"],
                prev_careunit=row["PREV_CAREUNIT"],
                curr_careunit=row["CURR_CAREUNIT"],
                prev_wardid=row["PREV_WARDID"],
                curr_wardid=row["CURR_WARDID"],
                intime=row["INTIME"],
                outtime=row["OUTTIME"],
                los=row["LOS"]
            )
            graph.merge(transfer_node, "Transfer", "row_id")

# Step 5: Create relationships between nodes
def create_relationships(admissions_df, diagnoses_df):
    """
    Create relationships between nodes in Neo4j.
    """
    print("Creating relationships between Patients and Admissions...")
    for _, row in tqdm(admissions_df.iterrows(), total=admissions_df.shape[0], desc="Patient-Admission Relationships"):
        patient_node = graph.nodes.match("Patient", patient_id=row["patient_id"]).first()
        admission_node = graph.nodes.match("Admission", admission_id=row["admission_id"]).first()
        if patient_node and admission_node:
            relationship = Relationship(patient_node, "HAS_ADMISSION", admission_node)
            graph.merge(relationship)

    print("Creating relationships between Admissions and Diagnoses...")
    for _, row in tqdm(diagnoses_df.iterrows(), total=diagnoses_df.shape[0], desc="Admission-Diagnosis Relationships"):
        admission_node = graph.nodes.match("Admission", admission_id=row["admission_id"]).first()
        diagnosis_node = graph.nodes.match("Diagnosis", diagnosis_id=row["diagnosis_id"]).first()
        if admission_node and diagnosis_node:
            relationship = Relationship(admission_node, "HAS_DIAGNOSIS", diagnosis_node)
            graph.merge(relationship)

# Main function to execute all steps
if __name__ == "__main__":
    # Step 1: Create Schema
    # create_schema()

    # Step 2: Load Data
    # chartevents_df = load_data()

    # Step 3: Create Nodes
    # create_nodes(chartevents_df)
    
    load_data()

    # Step 4: Create Relationships
    # create_relationships(admissions_df, diagnoses_df)

    print("MIMIC-III dataset loaded successfully into Neo4j!")
