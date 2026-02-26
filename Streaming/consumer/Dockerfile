FROM python:3.10-slim

WORKDIR /app

# Install dependencies
RUN pip install --no-cache-dir \
    kafka-python==2.0.2 \
    hdfs==2.7.0

# Copy consumer script
COPY consume_to_hdfs.py .

# Run consumer
CMD ["python", "-u", "consume_to_hdfs.py"]
