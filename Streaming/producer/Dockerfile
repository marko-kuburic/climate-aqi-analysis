FROM python:3.10-slim

WORKDIR /app

# Install dependencies
RUN pip install --no-cache-dir \
    kafka-python==2.0.2 \
    requests==2.31.0

# Copy producer script
COPY produce_air_quality_aqicn.py .

# Run the AQICN producer
CMD ["python", "-u", "produce_air_quality_aqicn.py"]
