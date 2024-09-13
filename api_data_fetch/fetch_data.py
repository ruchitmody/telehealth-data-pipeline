import requests
import pandas as pd

# Define the API endpoint
url = "https://api.fda.gov/drug/event.json?limit=100"

# Make the API request
response = requests.get(url)
data = response.json()

# Extract results and normalize into a Pandas DataFrame
results = data['results']
df = pd.json_normalize(results)

# Preview the data
print(df.head())

# Save the data to a CSV file
df.to_csv('data/raw/drug_data.csv', index=False)
