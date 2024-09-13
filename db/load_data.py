import psycopg2
import pandas as pd
import json
from datetime import datetime

# Load the CSV data
df = pd.read_csv('data/raw/drug_data.csv')

# Connect to PostgreSQL
conn = psycopg2.connect(
    host="localhost",
    database="healthcare_data",
    user="test_username",  # Replace with your PostgreSQL username
    password="test_password"  # Replace with your PostgreSQL password
)
cur = conn.cursor()

# Insert data into PostgreSQL
for i, row in df.iterrows():
    # Extract drug indication safely
    drug_indication = None
    try:
        # Parse the 'drug' column, assuming it's a JSON string
        drug_data = json.loads(row.get('drug', '[]'))
        if isinstance(drug_data, list) and len(drug_data) > 0:
            drug_indication = drug_data[0].get('drugindication', None)
    except (json.JSONDecodeError, TypeError):
        pass
    
    # Handle date formatting
    try:
        receivedate = pd.to_datetime(row.get('receivedate'), format='%Y%m%d').date()
    except ValueError:
        receivedate = None

    # Map DataFrame columns to table columns
    cur.execute("""
        INSERT INTO drug_data (safetyreportid, receivedate, patient_age, drug_indication, outcome)
        VALUES (%s, %s, %s, %s, %s)
    """, (row.get('safetyreportid'), receivedate, row.get('patientonsetage'), drug_indication, row.get('seriousnessdeath')))

# Commit changes and close the connection
conn.commit()
cur.close()
conn.close()

