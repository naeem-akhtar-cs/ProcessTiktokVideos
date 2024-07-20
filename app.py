import requests, json, subprocess, os, math, random
from datetime import datetime, timedelta
from dotenv import load_dotenv

from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload

from flask import Flask
from celery import Celery

app = Flask(__name__)

load_dotenv()

AIRTABLE_API_KEY = os.getenv("AIRTABLE_API_KEY")
AIRTABLE_BASE_ID = os.getenv("AIRTABLE_BASE_ID")
AIRTABLE_TABLE_ID = os.getenv("AIRTABLE_TABLE_ID")
AIRTABLE_VIEW_ID = os.getenv("AIRTABLE_VIEW_ID")
AIRTABLE_TABLE_ID_DRIVE = os.getenv("AIRTABLE_TABLE_ID_DRIVE")

GOOGLE_DRIVE_FOLDER_ID = os.getenv("GOOGLE_DRIVE_FOLDER_ID")

baseUrl = "https://api.airtable.com/v0"
driveDownloadBaseUrl = "https://drive.google.com/uc?export=download&id="

locations = {
    "New York": "+40.7128-074.0060/",
    "Chicago": "+41.8781-087.6298/",
    "Miami": "+25.7617-080.1918/",
    "Boston": "+42.3601-071.0589/",
    "Denver": "+39.7392-104.9903/",
    "San Francisco": "+37.7749-122.4194/",
    "Washington, D.C.": "+38.9072-077.0369/",
    "Houston": "+29.7604-095.3698/",
    "Las Vegas": "+36.1699-115.1398/",
    "San Jose": "+37.3382-121.8863/",
}

def make_celery(app):
    celery = Celery(
        app.import_name,
        backend=app.config['result_backend'],
        broker=app.config['CELERY_BROKER_URL']
    )
    celery.conf.update(app.config)
    return celery

app.config.update(
    CELERY_BROKER_URL='redis://redis:6379/0',
    result_backend='redis://redis:6379/0'
)

celery = make_celery(app)

def getAirtableRecords(offset):
    url = f"{baseUrl}/{AIRTABLE_BASE_ID}/{AIRTABLE_TABLE_ID}"
    headers = {"Authorization": f"Bearer {AIRTABLE_API_KEY}"}

    params = {"view": AIRTABLE_VIEW_ID, 'filterByFormula': '{Video Processed} = False()'}
    if offset is not None:
        params["offset"] = offset

    print(f"Records Offset: {offset}")

    try:
        response = requests.get(url, headers=headers, params=params)
        if response.status_code == 429: # Request rate limit case
            time.sleep(30)
            response = requests.get(url, headers=headers, params=params)
        response.raise_for_status()

        data = response.json()
        airtableData = {
            "records": data.get("records", []),
            "offset": data.get("offset"),
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
    fileName = f"{recordId}"
    response = requests.get(url=videoUrl, stream=True)
    with open(f"{folderName}/{fileName}.mp4", "wb") as writer:
        for chunk in response.iter_content(chunk_size=8192):
            writer.write(chunk)
    print(f"Video: {fileName}.mp4 downloaded")
    return fileName


def checkDir(folderName):
    currentDirectory = os.getcwd()
    folderPath = os.path.join(currentDirectory, folderName)
    if not os.path.exists(folderPath):
        os.makedirs(folderPath)
        print(f"Folder '{folderPath}' created.")


def removeFiles(folderName):
    currentDirectory = os.getcwd()
    folderPath = os.path.join(currentDirectory, folderName)
    for fileName in os.listdir(folderPath):
        filePath = os.path.join(folderPath, fileName)
        try:
            if os.path.isfile(filePath) or os.path.islink(filePath):
                os.unlink(filePath)
            elif os.path.isdir(filePath):
                shutil.rmtree(filePath)
        except Exception as e:
            print(f"Failed to delete {filePath}. Reason: {e}")


def removeFile(filePath):
    try:
        if os.path.isfile(filePath) or os.path.islink(filePath):
            os.unlink(filePath)
            print(f"File: {filePath} removed")
    except Exception as e:
        print(f"Failed to delete {filePath}. Reason: {e}")



def getVideoDimension(videPath):
    cmd = [
        "ffprobe",
        "-v",
        "error",
        "-select_streams",
        "v:0",
        "-count_packets",
        "-show_entries",
        "stream=width,height",
        "-of",
        "json",
        videPath,
    ]

    result = subprocess.run(
        cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True
    )

    if result.returncode != 0:
        print("Error:", result.stderr)
        return None

    data = json.loads(result.stdout)

    if "streams" in data and len(data["streams"]) > 0:
        stream = data["streams"][0]
        width = stream.get("width")
        height = stream.get("height")
        return {"width": width, "height": height}
    else:
        return None


def uploadToDrive(filePath, fileName):
    SERVICE_ACCOUNT_FILE = "creds.json"
    SCOPES = ["https://www.googleapis.com/auth/drive.file"]

    credentials = service_account.Credentials.from_service_account_file(SERVICE_ACCOUNT_FILE, scopes=SCOPES)

    service = build("drive", "v3", credentials=credentials)

    media = MediaFileUpload(filePath, resumable=True)

    fileMetadata = {"name": fileName, "parents": [GOOGLE_DRIVE_FOLDER_ID]}

    file = service.files().create(body=fileMetadata, media_body=media, fields="id").execute()

    fileUrl = driveDownloadBaseUrl + file.get("id")
    return fileUrl


def getVideoBitrate(filePath):
    cmd = [
        "ffprobe",
        "-v", "quiet",
        "-select_streams", "v:0",
        "-print_format", "json",
        "-show_entries", "stream=bit_rate",
        filePath,
    ]

    result = subprocess.run(cmd, capture_output=True, text=True)
    data = json.loads(result.stdout)

    return int(data["streams"][0]["bit_rate"])


def processVideo(processedVideos, fileName, processingSpecs):

    locationName, locationIso6709 = random.choice(list(locations.items()))

    randomDate = datetime.now() - timedelta(hours=random.randint(0, 24))
    dateStr = randomDate.strftime("%Y-%m-%dT%H:%M:%S")

    metadata = {
        "make": "Apple",
        "model": "iPhone 14 Pro",
        "location": locationName,
        "date": dateStr,
        "creation_time": dateStr,
        "com.apple.quicktime.make": "Apple",
        "com.apple.quicktime.model": "iPhone 14 Pro",
        "com.apple.quicktime.software": "17.4.1",
        "com.apple.quicktime.creationdate": f"{dateStr}+0000",
        "com.apple.quicktime.location.ISO6709": locationIso6709,
        "com.apple.quicktime.location.accuracy.horizontal": "6297.954794",
    }

    videoDimensions = getVideoDimension(f"{processedVideos}/{fileName}.mp4")
    bitrate = getVideoBitrate(f"{processedVideos}/{fileName}.mp4")
    bitrateKbps = f"{bitrate // 1000}k"

    angleRadians = math.radians(processingSpecs["rotationAngle"])

    sinTheta = math.sin(angleRadians)
    cosTheta = math.cos(angleRadians)

    newWidth = abs(videoDimensions["width"] * cosTheta) + abs(videoDimensions["height"] * sinTheta)
    newHeight = abs(videoDimensions["width"] * sinTheta) + abs(videoDimensions["height"] * cosTheta)

    updatedDimensions = {"width": newWidth, "height": newHeight}

    heightDiff = updatedDimensions["height"] - videoDimensions["height"]
    widthDiff = updatedDimensions["width"] - videoDimensions["width"]

    dimensionsDiff = {"width": heightDiff * 2, "height": widthDiff * 2}

    updatedDimensions = {
        "width": int(videoDimensions["width"] - dimensionsDiff["width"]),
        "height": int(videoDimensions["height"] - dimensionsDiff["height"]),
    }

    mirrorCommand = ""
    if processingSpecs["mirror"]:
        mirrorCommand = "hflip"

    ffmpegCommand = [
        "ffmpeg",
        "-i", f"{processedVideos}/{fileName}.mp4",
        "-vf", f'{mirrorCommand},rotate={processingSpecs["rotationAngle"]}*PI/180,crop={updatedDimensions["width"]}:{updatedDimensions["height"]},scale={videoDimensions["width"]}:{videoDimensions["height"]},eq=contrast={processingSpecs["contrast"]}:brightness={processingSpecs["brightness"]}:saturation={processingSpecs["saturation"]}:gamma={processingSpecs["gamma"]}',
        "-c:v", "libx264",
        "-b:v", bitrateKbps,
        "-c:a", "copy",
    ]

    for key, value in metadata.items():
        ffmpegCommand.extend(["-metadata", f"{key}={value}"])

    ffmpegCommand.append(f"{processedVideos}/{fileName}_{processingSpecs['variantId']}.mov")

    subprocess.run(ffmpegCommand, check=True, capture_output=True, text=True)

    randomNumber = random.randint(1000, 9999)
    fileUrl = uploadToDrive(f"{processedVideos}/{fileName}_{processingSpecs['variantId']}.mov", f"IMG_{randomNumber}.MOV")

    data = {
        "variantId": processingSpecs["variantId"],
        "fileUrl": fileUrl,
        "fileName": f"IMG_{randomNumber}.MOV",
        "randomNumber": randomNumber
    }
    return data


def addDataToAirTable(newRecordData):

    url = f"{baseUrl}/{AIRTABLE_BASE_ID}/{AIRTABLE_TABLE_ID_DRIVE}"
    headers = {
        "Authorization": f"Bearer {AIRTABLE_API_KEY}",
        "Content-Type": "application/json",
    }

    records = []
    for variant in newRecordData["variantsList"]:
        record = {
            "fields": {
                "Name": variant["fileName"],
                "Tiktok ID": [newRecordData["recordId"]],
                "Tiktok URL": newRecordData["tiktokUrl"],
                "sound url": newRecordData["soundUrl"],
                "Google Drive URL": variant["fileUrl"],
                "Variant Id": variant["variantId"],
                "Number": variant["randomNumber"]
            }
        }
        records.append(record)

    payload = json.dumps({"records": records})

    try:
        response = requests.request("POST", url, headers=headers, data=payload)
        if response.status_code == 429: # Request rate limit case
            time.sleep(30)
            response = requests.request("POST", url, headers=headers, data=payload)
        response.raise_for_status()

        data = response.json()

    except requests.exceptions.HTTPError as e:
        print(f"HTTP Error: {e}")
        print(f"Response content: {response.text}")
        return []

    except requests.exceptions.RequestException as e:
        print(f"Request Exception: {e}")
        return []


def updateRecordStatus(data):
    url = f"{baseUrl}/{AIRTABLE_BASE_ID}/{AIRTABLE_TABLE_ID}/{data['recordId']}"
    headers = {"Authorization": f"Bearer {AIRTABLE_API_KEY}", 'Content-Type': 'application/json',}

    payload = json.dumps({
        "fields": {
            "Video Processed": True
        }
    })

    try:
        response = requests.request("PATCH", url, headers=headers, data=payload)
        if response.status_code == 429: # Request rate limit case
            time.sleep(30)
            response = requests.request("PATCH", url, headers=headers, data=payload)
        response.raise_for_status()
        return True
    except requests.exceptions.HTTPError as e:
        print(f"HTTP Error: {e}")
        print(f"Response content: {response.text}")
        return False


# @celery.task()
def processVideoTask(record, processedVideos):

    recordId = record["id"]
    recordFields = record["fields"]
    fileName = downloadVideo(recordFields["Google Drive URL"], processedVideos, recordId)

    processingSpecs = [
        {"variantId": 1, "rotationAngle": 3, "contrast": 1.2, "brightness": -0.1, "saturation": 1, "gamma": 1.0, "mirror": True},
        {"variantId": 2, "rotationAngle": 2, "contrast": 1.8, "brightness": 0.1, "saturation": 2, "gamma": 2.0, "mirror": True},
        {"variantId": 3, "rotationAngle": 3, "contrast": 1.5, "brightness": 0.15, "saturation": 2, "gamma": 2.0, "mirror": False},
        {"variantId": 4, "rotationAngle": 0, "contrast": 1.8, "brightness": 0, "saturation": 1.5, "gamma": 1.2, "mirror": True},
        {"variantId": 5, "rotationAngle": -3, "contrast": 1.2, "brightness": -0.15, "saturation": 1, "gamma": 1, "mirror": False},
    ]

    variantsList = []
    for specs in processingSpecs:
        variant = processVideo(processedVideos, fileName, specs)
        variantsList.append(variant)

    newRecordData = {
        "recordId": recordId,
        "tiktokUrl": record["fields"]["Video URL"],
        "soundUrl": record["fields"]["short sound url"],
        "variantsList": variantsList
    }

    addDataToAirTable(newRecordData)
    status = updateRecordStatus({"recordId": recordId})
    if not status:
        print("Could not update status in linked table for record: {recordId}")
    
    for variant in variantsList:
        filePath = f"{processedVideos}/{fileName}_{variant['variantId']}.mov"
        removeFile(filePath)


@app.route('/')
def startProcessing():

    processedVideos = "ProcessedVideos"

    checkDir(processedVideos)
    removeFiles(processedVideos)

    offset = None
    firstRequest = True

    while offset is not None or firstRequest:
        data = getAirtableRecords(offset)
        records = data["records"]
        offset = data["offset"]

        print(f"Retrieved: {len(records)}")

        if records:
            for record in records:
                processVideoTask.delay(record, processedVideos)
        firstRequest = False

    return "Processing started."

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 8080)))
    print(f"App running at port 8080")
