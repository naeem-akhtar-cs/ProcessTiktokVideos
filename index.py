import requests, json, subprocess, os
from dotenv import load_dotenv

load_dotenv()

AIRTABLE_API_KEY = os.getenv('AIRTABLE_API_KEY')
AIRTABLE_BASE_ID = os.getenv('AIRTABLE_BASE_ID')
AIRTABLE_TABLE_ID = os.getenv('AIRTABLE_TABLE_ID')
AIRTABLE_VIEW_ID = os.getenv('AIRTABLE_VIEW_ID')

baseUrl = 'https://api.airtable.com/v0'


def getAirtableRecords(offset):
    url = f"{baseUrl}/{AIRTABLE_BASE_ID}/{AIRTABLE_TABLE_ID}"
    headers = {'Authorization': f'Bearer {AIRTABLE_API_KEY}'}

    params = {
        "view": AIRTABLE_VIEW_ID
    }
    if offset is not None:
        params["offset"] = offset

    print(f"Records Offset: {offset}")

    try:
        response = requests.get(url, headers=headers, params = params)
        response.raise_for_status()
        
        data = response.json()
        airtableData  = {
            "records": data.get('records', []),
            "offset": data.get("offset")
        }
        return airtableData

    except requests.exceptions.HTTPError as e:
        print(f"HTTP Error: {e}")
        print(f"Response content: {response.text}")
        return []

    except requests.exceptions.RequestException as e:
        print(f"Request Exception: {e}")
        return []


def downloadVideo(videoUrl, folderName, recordId):
    fileName = f"{recordId}.mp4"
    response = requests.get(url = videoUrl, stream=True)
    with open(f"{folderName}/{fileName}", "wb") as writer:
        for chunk in response.iter_content(chunk_size=8192):
            writer.write(chunk)
    print(f"Video: {fileName} downloaded")
    return fileName


def processVideo(downloadedVideos, processedVideos, fileName, processingSpecs):
    ffmpegCommand = [
        'ffmpeg',
        '-i', f"{downloadedVideos}/{fileName}",
        '-vf', f'rotate={processingSpecs["rotationAngle"]}*PI/180',
        '-metadata:s:v:0', f'rotate={processingSpecs["rotationAngle"]}',
        '-codec:a', 'copy',
        f"{processedVideos}/{fileName}",
    ]
    
    subprocess.run(ffmpegCommand, check=True)
    print(f"{processedVideos}/{fileName} processed and saved")


if __name__ == '__main__':
    downloadedVideos = "DownloadedVideos"
    processedVideos = "ProcessedVideos"

    currentDirectory = os.getcwd()

    folderPath = os.path.join(currentDirectory, downloadedVideos)
    if not os.path.exists(folderPath):
        os.makedirs(folderPath)
        print(f"Folder '{downloadedVideos}' created.")

    folderPath = os.path.join(currentDirectory, processedVideos)
    if not os.path.exists(folderPath):
        os.makedirs(folderPath)
        print(f"Folder '{processedVideos}' created.")

    offset = None
    firstRequest = True

    while offset is not None or firstRequest:
        data = getAirtableRecords(offset)
        records = data["records"]
        offset = data["offset"]

        if records:
            print(f"Retrieved {len(records)} records from Airtable")
            for record in records[:1]:
                recordId = record["id"]
                recordFields = record['fields']
                fileName = downloadVideo(recordFields["Google Drive URL"], downloadedVideos, recordId)

                processingSpecs = {
                    "rotationAngle": 3
                }

                processVideo(downloadedVideos, processedVideos, fileName, processingSpecs)
        else:
            print("No records retrieved from AirTable")
        firstRequest = False
