# import ffmpeg
import requests, json, subprocess, os, math
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


def checkDir(folderName):
    currentDirectory = os.getcwd()
    folderPath = os.path.join(currentDirectory, folderName)
    if not os.path.exists(folderPath):
        os.makedirs(folderPath)
        print(f"Folder '{downloadedVideos}' created.")


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
            print(f'Failed to delete {filePath}. Reason: {e}')


def getVideoDimension(videPath):
    cmd = [
        'ffprobe',
        '-v', 'error',
        '-select_streams', 'v:0',
        '-count_packets',
        '-show_entries', 'stream=width,height',
        '-of', 'json',
        videPath
    ]
    
    result = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    
    if result.returncode != 0:
        print("Error:", result.stderr)
        return None
    
    data = json.loads(result.stdout)
    
    if 'streams' in data and len(data['streams']) > 0:
        stream = data['streams'][0]
        width = stream.get('width')
        height = stream.get('height')
        return {"width": width, "height": height}
    else:
        return None


def processVideo(downloadedVideos, rotatedVideos, croppedVideos, resizedVideos, fileName, processingSpecs):

    videoDimensions = getVideoDimension(f"{downloadedVideos}/{fileName}")

    angleRadians = math.radians(processingSpecs["rotationAngle"])
    
    sinTheta = math.sin(angleRadians)
    cosTheta = math.cos(angleRadians)
    
    newWidth = abs(videoDimensions["width"] * cosTheta) + abs(videoDimensions["height"] * sinTheta)
    newHeight = abs(videoDimensions["width"] * sinTheta) + abs(videoDimensions["height"] * cosTheta)

    updateDimensions = {
        "width": int(newWidth),
        "height": int(newHeight)
    }

    heightDiff = updateDimensions["height"] - videoDimensions["height"]
    widthDiff = updateDimensions["width"] - videoDimensions["width"]

    dimensionsDiff = {
        "width": heightDiff * 2,
        "height": widthDiff * 2
    }

    updateDimensions = {
        "width": videoDimensions["width"] - dimensionsDiff["width"],
        "height": videoDimensions["height"] - dimensionsDiff["height"],
    }

    ffmpegCommand = [
        'ffmpeg',
        '-i', f"{downloadedVideos}/{fileName}",
        '-vf', f'rotate={processingSpecs["rotationAngle"]}*PI/180',
        '-metadata:s:v:0', f'rotate={processingSpecs["rotationAngle"]}',
        '-codec:a', 'copy',
        f"{rotatedVideos}/{fileName}",
    ]
    
    subprocess.run(ffmpegCommand, check=True)

    ffmpegCommand = [
        'ffmpeg',
        '-i', f"{rotatedVideos}/{fileName}",
        '-vf', f'crop={updateDimensions["width"]}:{updateDimensions["height"]}',
        '-metadata:s:v:0', f'rotate={processingSpecs["rotationAngle"]}',
        '-codec:a', 'copy',
        f"{croppedVideos}/{fileName}",
    ]
    
    subprocess.run(ffmpegCommand, check=True)

    ffmpegCommand = [
        'ffmpeg',
        '-i', f"{croppedVideos}/{fileName}",
        '-vf', f'crop={updateDimensions["width"]}:{updateDimensions["height"]}',
        '-metadata:s:v:0', f'scale={videoDimensions["width"]}:{videoDimensions["height"]}',
        '-codec:a', 'copy',
        f"{resizedVideos}/{fileName}",
    ]
    
    subprocess.run(ffmpegCommand, check=True)


    print(f"{fileName} processed and saved")

if __name__ == '__main__':
    downloadedVideos = "DownloadedVideos"
    rotatedVideos = "RotatedVideos"
    croppedVideos = "CroppedVideos"
    resizedVideos = "ResizedVideos"

    checkDir(downloadedVideos)
    removeFiles(downloadedVideos)

    checkDir(rotatedVideos)
    removeFiles(rotatedVideos)
    
    checkDir(croppedVideos)
    removeFiles(croppedVideos)

    checkDir(resizedVideos)
    removeFiles(resizedVideos)

    offset = None
    firstRequest = True

    while offset is not None or firstRequest:
        data = getAirtableRecords(offset)
        records = data["records"]
        offset = data["offset"]

        if records:
            print(f"Retrieved {len(records)} records from Airtable")
            for record in records[:2]:
                recordId = record["id"]
                recordFields = record['fields']
                fileName = downloadVideo(recordFields["Google Drive URL"], downloadedVideos, recordId)

                processingSpecs = {
                    "rotationAngle": 3
                }

                processVideo(downloadedVideos, rotatedVideos, croppedVideos, resizedVideos, fileName, processingSpecs)
                print(getVideoDimension(f"{downloadedVideos}/{fileName}"))
                print(getVideoDimension(f"{resizedVideos}/{fileName}"))
        else:
            print("No records retrieved from AirTable")
        firstRequest = False
