import requests, json, subprocess, os, math, random, uuid
from datetime import datetime, timedelta
from dotenv import load_dotenv

from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload

from flask import Flask, request, jsonify, send_file, after_this_request, make_response
from celery import Celery
from celery.result import AsyncResult

app = Flask(__name__)

load_dotenv()

AIRTABLE_API_KEY = os.getenv("AIRTABLE_API_KEY")
AIRTABLE_BASE_ID = os.getenv("AIRTABLE_BASE_ID")
AIRTABLE_TABLE_ID = os.getenv("AIRTABLE_TABLE_ID")
AIRTABLE_SPECS_TABLE_ID = os.getenv("AIRTABLE_SPECS_TABLE_ID")
AIRTABLE_VIEW_ID = os.getenv("AIRTABLE_VIEW_ID")
AIRTABLE_TABLE_ID_DRIVE = os.getenv("AIRTABLE_TABLE_ID_DRIVE")
AIRTABLE_LONG_FORMAT_TABLE_ID = os.getenv("AIRTABLE_LONG_FORMAT_TABLE_ID")
AIRTABLE_LONG_FORMAT_VIEW_ID = os.getenv("AIRTABLE_LONG_FORMAT_VIEW_ID")

# GOOGLE_DRIVE_FOLDER_ID = os.getenv("GOOGLE_DRIVE_FOLDER_ID")

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

def getAirtableRecords(offset, tableId, viewId, filterColumnName):
    url = f"{baseUrl}/{AIRTABLE_BASE_ID}/{tableId}"
    headers = {"Authorization": f"Bearer {AIRTABLE_API_KEY}"}

    params = {"view": viewId, 'filterByFormula': "{" + filterColumnName + "} = False()"}
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
        return {}

    except requests.exceptions.RequestException as e:
        print(f"Request Exception: {e}")
        return {}


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

    result = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)

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


def uploadToDrive(filePath, fileName, folderId):
    SERVICE_ACCOUNT_FILE = "creds.json"
    SCOPES = ["https://www.googleapis.com/auth/drive.file"]

    credentials = service_account.Credentials.from_service_account_file(SERVICE_ACCOUNT_FILE, scopes=SCOPES)

    service = build("drive", "v3", credentials=credentials)

    media = MediaFileUpload(filePath, resumable=True)

    fileMetadata = {"name": fileName, "parents": [folderId]}

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
    # bitrate = getVideoBitrate(f"{processedVideos}/{fileName}.mp4")
    # print(bitrate)
    # bitrateKbps = f"{(bitrate) // 1000}k"
    # print(bitrateKbps)

    angleRadians = math.radians(processingSpecs["RotationAngle"])

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
    mirrorVideo = processingSpecs.get("Mirror")
    if mirrorVideo is not None and mirrorVideo == True:
        mirrorCommand = "hflip,"

    ffmpegCommand = [
        "ffmpeg",
        "-i", f"{processedVideos}/{fileName}.mp4",
        "-vf", f'{mirrorCommand}rotate={processingSpecs["RotationAngle"]}*PI/180,crop={updatedDimensions["width"]}:{updatedDimensions["height"]},scale={videoDimensions["width"]}:{videoDimensions["height"]}:flags=lanczos,eq=contrast={processingSpecs["Contrast"]}:brightness={processingSpecs["Brightness"]}:saturation={processingSpecs["Saturation"]}:gamma={processingSpecs["Gamma"]}',
        "-c:v", "libx264",
        "-preset", "slow",
        "-crf", "18",
        "-c:a", "aac",
        "-b:a", "192k",
        "-movflags", "+faststart"
    ]

    for key, value in metadata.items():
        ffmpegCommand.extend(["-metadata", f"{key}={value}"])

    ffmpegCommand.append(f"{processedVideos}/{fileName}_{processingSpecs['VariantId']}.mov")

    subprocess.run(ffmpegCommand, check=True, capture_output=True, text=True)


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


def getProcessingSpecs():
    url = f"{baseUrl}/{AIRTABLE_BASE_ID}/{AIRTABLE_SPECS_TABLE_ID}"
    headers = {"Authorization": f"Bearer {AIRTABLE_API_KEY}"}

    try:
        response = requests.get(url, headers=headers)
        if response.status_code == 429: # Request rate limit case
            time.sleep(30)
            response = requests.get(url, headers=headers, params=params)
        response.raise_for_status()

        data = response.json()
        specsInfo =  data.get("records", [])
        specsList = []
        for specs in specsInfo:
            specsList.append(specs["fields"])
        return specsList
    except Exception as e:
        print(e)


@celery.task()
def processVideoTask(record, processedVideos, processingSpecs):
    recordId = record["id"]
    recordFields = record["fields"]
    variationFolderId = recordFields["drive folder Variations (from Model)"][0]
    fileName = downloadVideo(recordFields["Google Drive URL"], processedVideos, recordId)

    variantsList = []
    for specs in processingSpecs:
        processVideo(processedVideos, fileName, specs)

        randomNumber = random.randint(1000, 9999)
        fileUrl = uploadToDrive(f"{processedVideos}/{fileName}_{specs['VariantId']}.mov", f"IMG_{randomNumber}.MOV", variationFolderId)

        variant = {
            "variantId": specs["VariantId"],
            "fileUrl": fileUrl,
            "fileName": f"IMG_{randomNumber}.MOV",
            "randomNumber": randomNumber
        }
        variantsList.append(variant)

    newRecordData = {
        "recordId": recordId,
        "tiktokUrl": record["fields"]["Video URL"],
        "soundUrl": record["fields"]["short sound url"],
        "variantsList": variantsList,
        "DriveId": variationFolderId
    }

    addDataToAirTable(newRecordData)
    status = updateRecordStatus({"recordId": recordId})
    if not status:
        print(f"Could not update status in linked table for record: {recordId}")

    for variant in variantsList:
        filePath = f"{processedVideos}/{fileName}_{variant['variantId']}.mov"
        removeFile(filePath)


@app.route('/')
def startProcessing():
    processedVideos = "ProcessedVideos"

    checkDir(processedVideos)
    removeFiles(processedVideos)

    processingSpecs = getProcessingSpecs()

    offset = None
    firstRequest = True
    while offset is not None or firstRequest:
        data = getAirtableRecords(offset, AIRTABLE_TABLE_ID, AIRTABLE_VIEW_ID, "Video Processed")
        print(json.dumps(data))
        records = data["records"]
        offset = data["offset"]

        if records:
            for record in records:
                processVideoTask.delay(record, processedVideos, processingSpecs)
        firstRequest = False

    return jsonify({"status": 200, "message": "Processing started!!"})


@celery.task()
def downloadSingleVideo(processedVideos, data):
    videoUrl = data["videoUrl"]
    videoSpec = data["videoSpec"]
    uuidString = data["taskId"]

    videoSpec["VariantId"] = "Processed"

    fileName = downloadVideo(videoUrl, processedVideos, uuidString)
    processVideo(processedVideos, fileName, videoSpec)
    originalFilePath = f"{processedVideos}/{fileName}.mp4"
    removeFile(originalFilePath)


@app.route('/processSingleVideo', methods=['POST'])
def processSingleVideo():

    processedVideos = "ProcessSingleVideo"
    checkDir(processedVideos)

    data = request.get_json()
    taskId = data.get("taskId")

    if taskId is not None:
        taskResult = AsyncResult(taskId)
        status = taskResult.status
        if status == "SUCCESS":
            filePath = f"{processedVideos}/{taskId}_Processed.mov"
            if not os.path.exists(filePath):
                return "Error processing. Ask developer :)"

            @after_this_request
            def removeFile(response):
                try:
                    os.remove(filePath)
                    print(f"File removed: {filePath}")
                except Exception as error:
                    print(f"Error removing file: {error}")
                return response
            return send_file(filePath, mimetype='video/mp4')

        elif status == "FAILURE":
            return "Error processing. Ask developer :)"
        elif status == "PENDING":
            return "Processing in progress. Please wait :)"
        else:
            return "Unexpected behaviour. Please wait :)"

    uuId = str(uuid.uuid4())
    data["taskId"] = uuId

    task = downloadSingleVideo.apply_async(args = [processedVideos, data], task_id = uuId)
    taskId = task.id
    return taskId


def downloadVideoAuth(processedVideos, fileId, fileName):
    try:
        fileExtension = fileName.split(".")[-1]
        fileName = f"{fileId}.{fileExtension}"
        filePath = f"{processedVideos}/{fileName}"

        SERVICE_ACCOUNT_FILE = "creds.json"
        SCOPES = ["https://www.googleapis.com/auth/drive.readonly"]
        
        credentials = service_account.Credentials.from_service_account_file(SERVICE_ACCOUNT_FILE, scopes=SCOPES)
        service = build("drive", "v3", credentials=credentials)

        request = service.files().get_media(fileId=fileId)
        fh = io.BytesIO()
        downloader = MediaIoBaseDownload(fh, request)
        done = False
        while done is False:
            status, done = downloader.next_chunk()
            print(f"Download {filePath}: {int(status.progress() * 100)}%.")
        
        with open(filePath, 'wb') as f:
            f.write(fh.getvalue())
        return fileName
    except Exception as e:
        print(f"An error occurred while downloading {filePath}: {e}")


def splitVideo(folderName, fileName, splitLength):
    filePath = f"{folderName}/{fileName}"
    fileExtension = fileName.split(".")[-1]
    fileName = fileName.split(".")[0]

    ffprobeCommand = [
        "ffprobe", "-v", "error", "-show_entries", "format=duration",
        "-of", "default=noprint_wrappers=1:nokey=1", filePath
    ]
    output = subprocess.check_output(ffprobeCommand).decode("utf-8").strip()
    duration = float(output)

    numSegments = math.ceil(duration / splitLength)
    splittedVideos = []
    for i in range(numSegments):
        startTime = i * splitLength

        splittedFileName = f"{fileName}_{i:03d}.{fileExtension}"
        splittedVideos.append(splittedFileName)
        outputFile = f"{folderName}/{splittedFileName}"
        
        ffmpegCommand = [
            "ffmpeg", "-i", filePath, "-ss", str(startTime),
            "-t", str(splitLength), "-c", "copy", outputFile
        ]
        subprocess.run(ffmpegCommand, check=True)
    return splittedVideos


def updateSplitRecordStatus(recordId):
    url = f"{baseUrl}/{AIRTABLE_BASE_ID}/{longFormatTableId}/{recordId}"
    headers = {"Authorization": f"Bearer {AIRTABLE_API_KEY}", 'Content-Type': 'application/json',}

    payload = json.dumps({
        "fields": {
            "Processed": True
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


def addSplitDataToAirTable(newRecord):
    url = f"{baseUrl}/{AIRTABLE_BASE_ID}/{AIRTABLE_SHORT_FORMAT_TABLE_ID}"
    headers = {
        "Authorization": f"Bearer {AIRTABLE_API_KEY}",
        "Content-Type": "application/json",
    }

    records = [{ "fields": newRecord}]
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


@celery.task()
def processLongVideos(record, processedVideos):
    recordId = record["id"]
    recordFields = record["fields"]

    driveVideoUrl = record["fields"]["Google Drive URL"]

    parsedUrl = urlparse(driveVideoUrl)
    queryParams = parse_qs(parsedUrl.query)
    fileId = queryParams.get('id', [None])[0]
    if fileId is None:
        print(f"No id found in {driveVideoUrl}")
        return

    shortFormatFolder = record["fields"]["drive folder ShortFormat"][0]
    fileName = record["fields"]["Name"]
    splitLength = float(SPLIT_VIDEO_LENGTH)

    downloadedFileName = downloadVideoAuth(processedVideos, fileId, fileName)

    splittedVideos = splitVideo(processedVideos, downloadedFileName, splitLength)
    os.remove(f"{processedVideos}/{downloadedFileName}")
    fileNamePrefix = fileName.split(".")[0]

    for video in splittedVideos:
        filePath = f"{processedVideos}/{video}"
        fileIndex = video.split(".")[0].split("_")[-1]
        fileExtension = video.split(".")[-1]
        fileName = f"{fileNamePrefix}_{fileIndex}.{fileExtension}"
        fileUrl = uploadToDrive(filePath, fileName, shortFormatFolder)

        shortFormatRecord = {
            "Name": fileName,
            "Google Drive URL": fileUrl,
            "LongFormat": [recordId],
        }
        addSplitDataToAirTable(shortFormatRecord)
        removeFile(filePath)
    updateSplitRecordStatus(recordId)


@app.route('/splitVideos')
def splitVideos():
    processedVideos = "SplitVideos"

    checkDir(processedVideos)
    longFormatTableId = f"{AIRTABLE_LONG_FORMAT_TABLE_ID}"

    offset = None
    firstRequest = True
    while offset is not None or firstRequest:
        data = getAirtableRecords(offset, longFormatTableId, AIRTABLE_LONG_FORMAT_VIEW_ID, "Processed") # Getting data of long format videos
        print(json.dumps(data))
        records = data["records"]
        offset = data["offset"]

        if records:
            for record in records:
                if record["fields"].get("drive folder LongFormat") is not None:
                    processLongVideos.delay(record, processedVideos)
                    # processLongVideos(record, processedVideos)
        firstRequest = False

    return jsonify({"status": 200, "message": "Processing started!!"})


@app.route('/<path:path>')
def defaultRoute(path):
    return make_response(jsonify({"status": 404, "message": "Invalid route"}), 404)


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 8080)))
    print(f"App running at port 8080")
