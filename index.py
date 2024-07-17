import requests, json, subprocess, os, math
from dotenv import load_dotenv

from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload

load_dotenv()

AIRTABLE_API_KEY = os.getenv("AIRTABLE_API_KEY")
AIRTABLE_BASE_ID = os.getenv("AIRTABLE_BASE_ID")
AIRTABLE_TABLE_ID = os.getenv("AIRTABLE_TABLE_ID")
AIRTABLE_VIEW_ID = os.getenv("AIRTABLE_VIEW_ID")
GOOGLE_DRIVE_FOLDER_ID = os.getenv("GOOGLE_DRIVE_FOLDER_ID")

baseUrl = "https://api.airtable.com/v0"
driveDownloadBaseUrl = "https://drive.google.com/uc?export=download&id="

def getAirtableRecords(offset):
    url = f"{baseUrl}/{AIRTABLE_BASE_ID}/{AIRTABLE_TABLE_ID}"
    headers = {"Authorization": f"Bearer {AIRTABLE_API_KEY}"}

    params = {"view": AIRTABLE_VIEW_ID}
    if offset is not None:
        params["offset"] = offset

    print(f"Records Offset: {offset}")

    try:
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
        print(f"Folder '{processedVideos}' created.")


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
    SERVICE_ACCOUNT_FILE = "tiktok-drive-429704-9dcb46938fb6.json"
    SCOPES = ['https://www.googleapis.com/auth/drive.file']

    credentials = service_account.Credentials.from_service_account_file(SERVICE_ACCOUNT_FILE, scopes=SCOPES)

    service = build('drive', 'v3', credentials=credentials)

    media = MediaFileUpload(filePath, resumable=True)

    fileMetadata = {
        'name': fileName,
        'parents': [GOOGLE_DRIVE_FOLDER_ID]
    }

    file = service.files().create(
        body=fileMetadata,
        media_body=media,
        fields='id'
    ).execute()

    fileUrl = driveDownloadBaseUrl + file.get("id")
    return fileUrl


def processVideo(processedVideos, fileName, processingSpecs):

    videoDimensions = getVideoDimension(f"{processedVideos}/{fileName}.mp4")

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

    ffmpegCommand = [
        "ffmpeg",
        "-i", f"{processedVideos}/{fileName}.mp4",
        "-vf", f'rotate={processingSpecs["rotationAngle"]}*PI/180,crop={updatedDimensions["width"]}:{updatedDimensions["height"]},scale={videoDimensions["width"]}:{videoDimensions["height"]},eq=contrast={processingSpecs["contrast"]}:brightness={processingSpecs["brightness"]}:saturation={processingSpecs["saturation"]}:gamma={processingSpecs["gamma"]}',
        "-c:a", "copy",
        f"{processedVideos}/{fileName}_{processingSpecs['variantId']}.mov",
    ]
    subprocess.run(ffmpegCommand, check=True)

    fileUrl = uploadToDrive(f"{processedVideos}/{fileName}_{processingSpecs['variantId']}.mov", f"{fileName}_{processingSpecs['variantId']}.mov")
    return fileUrl


if __name__ == "__main__":
    processedVideos = "ProcessedVideos"

    checkDir(processedVideos)
    removeFiles(processedVideos)

    offset = None
    firstRequest = True

    contrastEquation = {
        'mild': 1.2,
        'moderate': 1.5,
        'aggressive': 1.8
    }

    # FFMPEG
    # Brightness range: -1.0 to +1.0, default: 0
    # Saturation range: 0.0 to 3.0, default: 1
    # Gamma range: 0.1 to 3.0, default 1

    # Website range
    # Brightness range: -1 to +2.5
    # Saturation range: 1 to 6
    # Gamma range: 2 to 6

    # processingSpecs = [
    #     {
    #         "rotationAngle": 3,
    #         "contrast": "mild",
    #         "brightness": -1,
    #         "saturation": 1,
    #         "gamma": 1,
    #     },
    #     {
    #         "rotationAngle": 2,
    #         "contrast": "aggressive",
    #         "brightness": 2,
    #         "saturation": 7,
    #         "gamma": 8,
    #     },
    #     {
    #         "rotationAngle": 3,
    #         "contrast": "moderate",
    #         "brightness": 1.5,
    #         "saturation": 7,
    #         "gamma": 8,
    #     },
    #     {
    #         "rotationAngle": 0,
    #         "contrast": "aggressive",
    #         "brightness": 0,
    #         "saturation": 5,
    #         "gamma": 3,
    #     },
    #     {
    #         "rotationAngle": -3,
    #         "contrast": "mild",
    #         "brightness": -1.5,
    #         "saturation": 1,
    #         "gamma": 2,
    #     },
    # ]

    processingSpecs = [
        {
            "variantId": 1,
            "rotationAngle": 3,
            "contrast": 1.2,
            "brightness": -0.1,
            "saturation": 1,
            "gamma": 1.0,
        },
        {
            "variantId": 2,
            "rotationAngle": 2,
            "contrast": 1.8,
            "brightness": 0.1,
            "saturation": 2,
            "gamma": 2.0,
        },
        {
            "variantId": 3,
            "rotationAngle": 3,
            "contrast": 1.5,
            "brightness": 0.15,
            "saturation": 2,
            "gamma": 2.0,
        },
        {
            "variantId": 4,
            "rotationAngle": 0,
            "contrast": 1.8,
            "brightness": 0,
            "saturation": 1.5,
            "gamma": 1.2,
        },
        {
            "variantId": 5,
            "rotationAngle": -3,
            "contrast": 1.2,
            "brightness": -0.15,
            "saturation": 1,
            "gamma": 1,
        },
    ]

    while offset is not None or firstRequest:
        data = getAirtableRecords(offset)
        records = data["records"]
        offset = data["offset"]

        if records:
            print(f"Retrieved {len(records)} records from Airtable")
            for record in records[:1]:
                recordId = record["id"]
                recordFields = record["fields"]
                fileName = downloadVideo(recordFields["Google Drive URL"], processedVideos, recordId)

                variantsUrls = []
                for specs in processingSpecs:
                    fileUrl = processVideo(processedVideos, fileName, specs)
                    variantsUrls.append(fileUrl)
                    # Add drive URL and other info to AirTable

                    print(f"{specs} processed")
                removeFiles(processedVideos)
                print(variantsUrls)
        else:
            print("No records retrieved from AirTable")
        firstRequest = False
