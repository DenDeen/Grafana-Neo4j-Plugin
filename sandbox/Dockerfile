FROM python:3.7-slim
WORKDIR /opt/app
COPY . .
RUN pip install --no-cache-dir -r requirements-prod.txt

# Environment variables
ENV NEO4J_HOST=host.docker.internal
ENV NEO4J_PORT=7687
ENV NEO4J_USERNAME=neo4j
ENV NEO4J_PASSWORD=abiss
ENV NEO4J_DATABASE_NAME=powerplant

EXPOSE 5000
CMD ["python3", "-m", "flask", "run", "--host=0.0.0.0"]