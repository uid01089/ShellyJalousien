pip freeze > requirements.txt
docker build -t shellyjalousien -f Dockerfile .
docker tag shellyjalousien:latest docker.diskstation/shellyjalousien
docker push docker.diskstation/shellyjalousien:latest