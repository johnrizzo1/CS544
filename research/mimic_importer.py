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



# Step 2: Define the schema
def create_schema():
    """
    Create constraints and indexes for the Neo4j database schema.
    """
    print("Creating schema...")
    # Drop constraints / indices
    graph.run("CREATE CONSTRAINT IF NOT EXISTS ON (ce:ChartEvent) ASSERT ce.row_id IS UNIQUE;")
    graph.run("CREATE CONSTRAINT IF NOT EXISTS ON (p:Patient) ASSERT p.patient_id IS UNIQUE;")
    graph.run("CREATE CONSTRAINT IF NOT EXISTS ON (a:Admission) ASSERT a.admission_id IS UNIQUE;")
    graph.run("CREATE CONSTRAINT IF NOT EXISTS ON (d:Diagnosis) ASSERT d.diagnosis_id IS UNIQUE;")
    graph.run("CREATE CONSTRAINT IF NOT EXISTS ON (l:LabEvent) ASSERT l.lab_id IS UNIQUE;")
    graph.run("CREATE CONSTRAINT IF NOT EXISTS ON (p:Prescription) ASSERT p.prescription_id IS UNIQUE;")
    print("Schema created successfully!")

# Step 3: Load and preprocess data
def load_data():
    """
    Load data from CSV files into Pandas DataFrames.
    Replace 'path_to_csv' with the actual path to your CSV files.
    """

    print("Loading data...")
    for chunk in pd.read_csv(f"{CSV_PATH}/CHARTEVENTS.csv", chunksize=10000):
        print("Inserting chunk...")
        # print(chunk.head())
        for _, row in tqdm(chunk.iterrows(), total=chunk.shape[0], desc="ChartEvents"):
            chartevent_node = Node(
                "ChartEvent",
                row_id=row["ROW_ID"],
                subject_id=row["SUBJECT_ID"],
                hadm_id=row["HADM_ID"],
                icustay_id=row["ICUSTAY_ID"],
                itemid=row["ITEMID"],
                charttime=row["CHARTTIME"],
                storetime=row["STORETIME"],
                cgid=row["CGID"],
                value=row["VALUE"],
                valuenum=row["VALUENUM"],
                valueuom=row["VALUEUOM"],
                warning=row["WARNING"],
                error=row["ERROR"],
                resultstatus=row["RESULTSTATUS"],
                stopped=row["STOPPED"]
            )
            graph.merge(chartevent_node, "ChartEvent", "row_id")  # Use merge to avoid duplicates

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
