import requests, json, subprocess, os, math, random, uuid, io, time, shutil, sys

import cv2
import numpy as np

from urllib.parse import urlparse, parse_qs
from datetime import datetime, timedelta
from dotenv import load_dotenv

from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload
from googleapiclient.http import MediaIoBaseDownload

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
AIRTABLE_SHORT_FORMAT_TABLE_ID = os.getenv("AIRTABLE_SHORT_FORMAT_TABLE_ID")
SPLIT_VIDEO_LENGTH = os.getenv("SPLIT_VIDEO_LENGTH")
USER_ACCOUNT_EMAIL = os.getenv("USER_ACCOUNT_EMAIL")

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

def getAirtableRecords(offset, tableId, viewId, filterColumns):
    url = f"{baseUrl}/{AIRTABLE_BASE_ID}/{tableId}"
    headers = {"Authorization": f"Bearer {AIRTABLE_API_KEY}"}

    # params = {"view": viewId, 'filterByFormula': "{" + filterColumnName + "} = False()"}

    columnNames = list(filterColumns.keys())

    params = {"view": viewId, 'filterByFormula': "{" + columnNames[0] + "} = " + str(filterColumns[columnNames[0]]) + "()"}
    if len(columnNames) > 1:
        params['filterByFormula'] = "AND({" + columnNames[0] + "} = " + str(filterColumns[columnNames[0]]) + "(), {" + columnNames[1] + "} = " + str(filterColumns[columnNames[1]]) + "())"
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


def getVideoInfo(videoPath):
    cmd = [
        "ffprobe",
        "-v",
        "error",
        "-select_streams",
        "v:0",
        "-count_packets",
        "-show_entries",
        "stream=width,height",
        "-show_entries",
        "format=duration",
        "-of",
        "json",
        videoPath,
    ]

    result = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)

    if result.returncode != 0:
        print("Error:", result.stderr)
        return None

    data = json.loads(result.stdout)

    if "streams" in data and len(data["streams"]) > 0 and "format" in data:
        stream = data["streams"][0]
        width = stream.get("width")
        height = stream.get("height")
        duration = int(float(data["format"].get("duration", 0)))
        return {"width": width, "height": height, "duration": duration}
    else:
        return None


def uploadToDrive(filePath, fileName, folderId):
    SERVICE_ACCOUNT_FILE = "creds.json"
    SCOPES = ["https://www.googleapis.com/auth/drive.file"]

    userAccountEmail = USER_ACCOUNT_EMAIL
    credentials = service_account.Credentials.from_service_account_file(SERVICE_ACCOUNT_FILE, scopes=SCOPES, subject=userAccountEmail)

    service = build("drive", "v3", credentials=credentials)
    media = MediaFileUpload(filePath, resumable=True)
    fileMetadata = {"name": fileName, "parents": [folderId]}
    file = service.files().create(body = fileMetadata, media_body = media, fields = "id").execute()
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


def deleteRandomPixels(folderName, fileName, variantId):
    inputVideo = f"{folderName}/{fileName}.mp4"
    tempVideoWithoutAudio = f"{folderName}/{fileName}_no_audio.mp4"
    outputVideo = f"{folderName}/{fileName}_pixels.mp4"

    cap = cv2.VideoCapture(inputVideo)
    fourcc = cv2.VideoWriter_fourcc(*'mp4v')
    frameWidth = int(cap.get(cv2.CAP_PROP_FRAME_WIDTH))
    frameHeight = int(cap.get(cv2.CAP_PROP_FRAME_HEIGHT))
    fps = int(cap.get(cv2.CAP_PROP_FPS))

    # algoId = random.randint(1, 3)
    algoId = variantId
    percentage = 0.01

    out = cv2.VideoWriter(tempVideoWithoutAudio, fourcc, fps, (frameWidth, frameHeight))
    while cap.isOpened():
        ret, frame = cap.read()
        if not ret:
            break
        frame = deleteRandomPixelsInFrame(frame, frameHeight, frameWidth, algoId, percentage)
        out.write(frame)
    cap.release()
    out.release()
    mergeAudioWithVideo(inputVideo, tempVideoWithoutAudio, outputVideo)
    return f"{fileName}_pixels"


def mergeAudioWithVideo(originalVideo, processedVideo, outputVideo):
    ffmpegCommand = [
        "ffmpeg",
        "-i", processedVideo,
        "-i", originalVideo,
        "-c:v", "copy",
        "-c:a", "aac",
        "-map", "0:v:0",
        "-map", "1:a:0",
        "-shortest",
        outputVideo
    ]
    subprocess.run(ffmpegCommand, check=True)


def deleteRandomPixelsInFrame(frame, frameHeight, frameWidth, originalAlgoId, percentage=0.01):
    totalPixels = frameHeight * frameWidth
    numPixelsToDelete = int(totalPixels * percentage)

    for _ in range(numPixelsToDelete):
        x = random.randint(0, frameWidth - 1)
        y = random.randint(0, frameHeight - 1)

        if originalAlgoId == 2:
            algoId = random.choice([1, 3, 4])
        else:
            algoId = originalAlgoId

        if algoId == 1:
            averageColor = getAverageColor(frame, x, y, frameHeight, frameWidth)
        elif algoId == 3:
            averageColor = getMedianColor(frame, x, y, frameHeight, frameWidth)
        elif algoId == 4:
            averageColor = getWeightedAverageColor(frame, x, y, frameHeight, frameWidth)
        # if algoId == 5:
        #     averageColor = getAverageColor(frame, x, y, frameHeight, frameWidth)

        frame[y, x] = averageColor
    return frame


# Detected on upload - Not working
def modifyPixelColor(frame, x, y, frameHeight, frameWidth):
    originalColor = frame[y, x]
    randomAdjustment = np.random.randint(-10, 11, size=3)
    modifiedColor = originalColor + randomAdjustment
    modifiedColor = np.clip(modifiedColor, 0, 255)
    return modifiedColor


def getAverageColor(frame, x, y, frameHeight, frameWidth):
    xMin = max(0, x - 1)
    xMax = min(frameWidth - 1, x + 1)
    yMin = max(0, y - 1)
    yMax = min(frameHeight - 1, y + 1)
    neighboringPixels = frame[yMin:yMax + 1, xMin:xMax + 1]
    averageColor = np.mean(neighboringPixels, axis=(0, 1)).astype(int)
    return averageColor


def getMedianColor(frame, x, y, frameHeight, frameWidth):
    xMin = max(0, x - 1)
    xMax = min(frameWidth - 1, x + 1)
    yMin = max(0, y - 1)
    yMax = min(frameHeight - 1, y + 1)
    neighboringPixels = frame[yMin:yMax + 1, xMin:xMax + 1]
    medianColor = np.median(neighboringPixels, axis=(0, 1)).astype(int)
    return medianColor


def getWeightedAverageColor(frame, x, y, frameHeight, frameWidth):
    xMin = max(0, x - 1)
    xMax = min(frameWidth - 1, x + 1)
    yMin = max(0, y - 1)
    yMax = min(frameHeight - 1, y + 1)
    neighboringPixels = frame[yMin:yMax + 1, xMin:xMax + 1]
    weights = np.array([
        [1, 2, 1],
        [2, 4, 2],
        [1, 2, 1]
    ])
    weights = weights[(yMin - y + 1):(yMax - y + 2), (xMin - x + 1):(xMax - x + 2)]
    weightedSum = np.tensordot(neighboringPixels, weights, axes=((0, 1), (0, 1)))
    weightedAverageColor = (weightedSum / np.sum(weights)).astype(int)
    return weightedAverageColor


def swapColumns(frame, startCol1, endCol1, startCol2, endCol2):
    temp = frame[:, startCol1:endCol1].copy()
    frame[:, startCol1:endCol1] = frame[:, startCol2:endCol2]
    frame[:, startCol2:endCol2] = temp
    return frame


def swapVideoSides(processedVideos, fileName):
    inputFilePath = f"{processedVideos}/{fileName}.mp4"
    cap = cv2.VideoCapture(inputFilePath)
    width = int(cap.get(cv2.CAP_PROP_FRAME_WIDTH))
    height = int(cap.get(cv2.CAP_PROP_FRAME_HEIGHT))
    fps = cap.get(cv2.CAP_PROP_FPS)
    fourcc = cv2.VideoWriter_fourcc(*'mp4v')

    outputFilePath = f"{processedVideos}/{fileName}_cut.mp4"
    out = cv2.VideoWriter(outputFilePath, fourcc, fps, (width, height))

    colsToSwap = 20
    startColLeft = int(width * 0.15) + colsToSwap
    endColLeft = startColLeft + colsToSwap
    startColRight = int(width * 0.85)
    endColRight = startColRight + colsToSwap

    while cap.isOpened():
        ret, frame = cap.read()
        if not ret:
            break
        # TODO Check width of video before swap
        frame = swapColumns(frame, startColLeft, endColLeft, endColLeft + 20, endColLeft + colsToSwap + 20)
        frame = swapColumns(frame, startColRight, endColRight, endColRight + 20, endColRight + colsToSwap + 20)
        out.write(frame)
    cap.release()
    out.release()
    cv2.destroyAllWindows()
    outputVideoUpdated = f"{processedVideos}/{fileName}_cut_audio.mp4"
    mergeAudioWithVideo(inputFilePath, outputFilePath, outputVideoUpdated)
    return f"{fileName}_cut_audio"


def sharpenVideo(inputFile, outputFile):
    sharpness = 4
    if not os.path.exists(inputFile):
        raise FileNotFoundError(f"Input file not found: {inputFile}")

    ffmpeg_cmd = [
        "ffmpeg",
        "-i", inputFile,
        "-filter:v", f"unsharp=5:5:{sharpness}:5:5:0",
        "-c:a", "copy",
        outputFile
    ]

    try:
        subprocess.run(ffmpeg_cmd, check=True)
    except subprocess.CalledProcessError as e:
        print(f"Error occurred while sharpening video: {e}")


def processVideo(processedVideos, fileName, processingSpecs):
    locationName, locationIso6709 = random.choice(list(locations.items()))

    randomDate = datetime.now() - timedelta(hours=random.randint(0, 24))
    dateStr = randomDate.strftime("%Y-%m-%dT%H:%M:%S")

    variantId = processingSpecs["VariantId"]
    fileName = deleteRandomPixels(processedVideos, fileName, variantId)

    # fileName = "recUk02J1czaRqI6J_pixels"

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

    videoDimensions = getVideoInfo(f"{processedVideos}/{fileName}.mp4")
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

    zoomEffect = ""
    # if variantId == 5:
    #     fileName = swapVideoSides(processedVideos, fileName)

    # if variantId ==  3:
    #     sharpenVideo(f"{processedVideos}/{fileName}.mp4", f"{processedVideos}/{fileName}_sharped.mp4")
    #     removeFile(f"{processedVideos}/{fileName}.mp4")
    #     os.rename(f"{processedVideos}/{fileName}_sharped.mp4", f"{processedVideos}/{fileName}.mp4")
    # if variantId ==  4:

    if variantId == 3 or variantId ==  4:
        startingPoint = random.randint(0, videoDimensions["duration"] - 5)
        zoomEffect = f"zoompan=z='if(gte(time,{startingPoint}),if(lt(time,{startingPoint}+2),1+((time-{startingPoint})/2),if(lt(time,{startingPoint}+3),2,if(lt(time,{startingPoint}+5),2-((time-{startingPoint}-3)/2),1))),1)':d=1:x='iw/2-(iw/zoom/2)':y='ih/2-(ih/zoom/2)':s={videoDimensions['width']}x{videoDimensions['height']}:fps=30,"
    elif variantId == 1:
        zoomEffect = f"zoompan=z='if(lt(time,2),2-(time/2),1)':d=1:x='iw/2-(iw/zoom/2)':y='ih/2-(ih/zoom/2)':s={videoDimensions['width']}x{videoDimensions['height']}:fps=30,"
    # elif variantId != 2  and variantId != 3 and videoDimensions["duration"]  >= 5:
    elif variantId != 2 and videoDimensions["duration"]  >= 5:
        zoomEffect = f"zoompan=z='if(lt(time,2),2-(time/2),1)':d=1:x='iw/2-(iw/zoom/2)':y='ih/2-(ih/zoom/2)':s={videoDimensions['width']}x{videoDimensions['height']}:fps=30,"

    ffmpegCommand = [
        "ffmpeg",
        "-i", f"{processedVideos}/{fileName}.mp4",
        "-vf", f'{mirrorCommand}{zoomEffect}rotate={processingSpecs["RotationAngle"]}*PI/180,crop={updatedDimensions["width"]}:{updatedDimensions["height"]},scale={videoDimensions["width"]}:{videoDimensions["height"]}:flags=lanczos,eq=contrast={processingSpecs["Contrast"]}:brightness={processingSpecs["Brightness"]}:saturation={processingSpecs["Saturation"]}:gamma={processingSpecs["Gamma"]}',
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

    try:
        subprocess.run(ffmpegCommand, check=True, capture_output=True, text=True)
    except subprocess.CalledProcessError as e:
        print("FFmpeg error:", e.stderr)
        raise
    removeFile(f"{processedVideos}/{fileName}.mp4")
    return fileName


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


def updateRecordStatus(data, filterColumns):
    url = f"{baseUrl}/{AIRTABLE_BASE_ID}/{AIRTABLE_TABLE_ID}/{data['recordId']}"
    headers = {"Authorization": f"Bearer {AIRTABLE_API_KEY}", 'Content-Type': 'application/json',}

    columnNames = list(filterColumns.keys())

    fields = {}
    for columnName in columnNames:
        fields[columnName] = filterColumns[columnName]

    payload = json.dumps({"fields": fields})

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
            response = requests.get(url, headers=headers)
        response.raise_for_status()

        data = response.json()
        specsInfo =  data.get("records", [])
        specsList = []
        for specs in specsInfo:
            specsList.append(specs["fields"])
        return specsList
    except Exception as e:
        print(e)
        return None


@celery.task()
def processVideoTask(record, processedVideos, processingSpecs):
    recordId = record["id"]
    recordFields = record["fields"]
    variationFolderId = recordFields["drive folder Variations (from Model)"][0]
    originalFileName = downloadVideo(recordFields["Google Drive URL"], processedVideos, recordId)

    variantsList = []
    # processingSpecs = [processingSpecs[3]]
    for specs in processingSpecs:
        fileName = processVideo(processedVideos, originalFileName, specs)

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
    status = updateRecordStatus({"recordId": recordId}, {"Video Processed": True, "Processing In Progress": False})
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
    if processingSpecs is None:
        print("Could not get processing specs")
        processingSpecs = getProcessingSpecs()
        if processingSpecs is None:
            print("Could not get processing specs for second time")
            processingSpecs = getProcessingSpecs()
            if processingSpecs is None:
                return jsonify({"status": 500, "message": "Error getting processing specs, please try again"})

    offset = None
    firstRequest = True
    while offset is not None or firstRequest:
        data = getAirtableRecords(offset, AIRTABLE_TABLE_ID, AIRTABLE_VIEW_ID, {"Video Processed": False,  "Processing In Progress": False})
        records = data.get("records")
        offset = data.get("offset")
        print("Records to Process")
        print(json.dumps(records))

        if records:
            for record in records:
                updateRecordStatus({"recordId": record["id"]}, {"Processing In Progress": True})
                processVideoTask.delay(record, processedVideos, processingSpecs)
                # processVideoTask(record, processedVideos, processingSpecs)
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

        userAccountEmail = USER_ACCOUNT_EMAIL
        credentials = service_account.Credentials.from_service_account_file(SERVICE_ACCOUNT_FILE, scopes=SCOPES, subject=userAccountEmail)
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
    url = f"{baseUrl}/{AIRTABLE_BASE_ID}/{AIRTABLE_LONG_FORMAT_TABLE_ID}/{recordId}"
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

    clipLength = record["fields"]["clip length"]
    if clipLength is not None:
        splitLength = int(clipLength)
    else:
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

    offset = None
    firstRequest = True
    while offset is not None or firstRequest:
        data = getAirtableRecords(offset, AIRTABLE_LONG_FORMAT_TABLE_ID, AIRTABLE_LONG_FORMAT_VIEW_ID, {"Processed": False}) # Getting data of long format videos
        records = data.get("records")
        offset = data.get("offset")

        print("Long videos")
        print(json.dumps(records))

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
