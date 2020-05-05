# docker build -t datacatalog-object-storage-processor .
FROM python:3.7

# Set the GOOGLE_APPLICATION_CREDENTIALS environment variable.
# At run time, /data must be binded to a volume containing a valid Service Account credentials file
# named datacatalog-object-storage-processor-sa.json.
ENV GOOGLE_APPLICATION_CREDENTIALS=/data/datacatalog-object-storage-processor-sa.json

WORKDIR /app

# Copy project files (see .dockerignore).
COPY . .

# Install gcs-file-creator package from source files.
RUN pip install .

ENTRYPOINT ["datacatalog-object-storage-processor"]
