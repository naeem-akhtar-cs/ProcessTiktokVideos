# Video Processing Application

## Overview

This application processes videos by applying various effects and uploading them to Google Drive. It's designed to work with Airtable for managing video data and uses Celery for handling background tasks.

## Features

- Fetches video information from Airtable
- Downloads videos from provided URLs
- Applies various effects to videos (rotation, mirroring, contrast adjustments, etc.)
- Adds metadata to processed videos (like location and device information)
- Uploads processed videos to Google Drive
- Updates Airtable with processed video information

## How It Works

1. The app retrieves video data from Airtable.
2. It downloads each video and applies specified effects.
3. Processed videos are uploaded to Google Drive.
4. Airtable is updated with new video information.

## Requirements

- Python 3.x
- Flask (web framework)
- Celery (task queue)
- Redis (message broker for Celery)
- FFmpeg (for video processing)
- Google Drive API credentials
- Airtable API key and base information

## Setup

1. Install required Python packages (listed in a requirements.txt file).
2. Set up environment variables for API keys and other sensitive information.
3. Ensure FFmpeg is installed on the system.
4. Set up Google Drive API credentials (save as `creds.json`).
5. Configure Redis for Celery.

## Usage

- Start the Flask application to begin processing videos from Airtable.
- Use the `/processSingleVideo` endpoint to process individual videos.

## Note for Non-Technical Users

This application requires some technical setup and is designed to run on a server. If you're not familiar with Python, APIs, or server management, you may need assistance from the developer to set up and run this application.

## Caution

- Be aware of API rate limits for Airtable and Google Drive.
- Ensure you have proper permissions for accessing and modifying data in Airtable and Google Drive.
- Video processing can be resource-intensive, so ensure your server has adequate capacity.

For any technical issues or advanced configuration, please consult with a developer or refer to the documentation of the libraries used in this project.

## How to Run the Application

After setting up the environment and installing all requirements, follow these steps to run the application:

```docker-compose down && docker-compose build && docker-compose up -d && docker-compose logs -f```

```docker-compose logs```
```docker-compose logs -f```
