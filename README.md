List containers: docker ps
Stop container: docker stop <container_id>
Show logs: docker logs <container_id>
Real time logs: docker logs -f <container_id>

docker-compose down && docker-compose build && docker-compose up -d && docker-compose logs -f

docker-compose logs
docker-compose logs -f


# TODO
1. Scheduler - Done
2. Test endpoint - done
3. Get params from airtable - done
4. Quality of video


curl --location 'http://127.0.0.1:8080/processSingleVideo' \
--header 'Content-Type: application/json' \
--data '{
    "videoUrl": "https://drive.google.com/uc?id=13TSPEn7x422AYiATdSLLKidy0djeWeh6&export=download",
    "variantId": 2
}' \
--output video.mov
