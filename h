# 1. Is MinIO running and healthy?
docker ps --filter "name=minio"

# 2. Can spark worker actually reach minio?
docker exec -it <spark-worker-container-name> sh -c "curl -v http://minio:9000/minio/health/live"

# 3. Are they on the same docker network?
docker network inspect <your-compose-network-name>
