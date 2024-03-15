export AIRFLOW_UID=5000
docker-compose down
docker builder prune -f --all
docker rmi $(docker images -aq)
docker-compose up -d 