import requests, json
import os
from dotenv import load_dotenv

load_dotenv()

AIRTABLE_API_KEY = os.getenv('AIRTABLE_API_KEY')
AIRTABLE_BASE_ID = os.getenv('AIRTABLE_BASE_ID')
AIRTABLE_TABLE_ID = os.getenv('AIRTABLE_TABLE_NAME')
AIRTABLE_VIEW_ID = os.getenv('AIRTABLE_VIEW_ID')

baseUrl = 'https://api.airtable.com/v0'


def getAirtableRecords():
    url = f"{baseUrl}/{AIRTABLE_BASE_ID}/{AIRTABLE_TABLE_ID}"
    headers = {'Authorization': f'Bearer {AIRTABLE_API_KEY}'}
    params = {
        "view": AIRTABLE_VIEW_ID
    }
    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        
        data = response.json()
        return data.get('records', [])

    except requests.exceptions.HTTPError as e:
        print(f"HTTP Error: {e}")
        print(f"Response content: {response.text}")
        return []

    except requests.exceptions.RequestException as e:
        print(f"Request Exception: {e}")
        return []

if __name__ == '__main__':
    
    records = getAirtableRecords()
    
    if records:
        print(f"Retrieved {len(records)} records from Airtable")
        for record in records[:5]:
            print(record['fields'])
    else:
        print("No records retrieved. Check the error messages above.")
