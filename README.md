docker build -t processvideos . && docker run -d -p 80:5000 processvideos

List containers: docker ps
Show logs: docker logs <container_id>
Real time logs: docker logs -f <container_id>
