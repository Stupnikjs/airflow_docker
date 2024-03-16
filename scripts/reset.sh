docker-compose down
docker builder prune -f --all
docker rmi $(docker images -aq)
docker-compose up -d 
# sudo chown -R mame /home/mame/airflow_project