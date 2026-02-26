# Custom Spark image with climate data processing libraries
FROM bde2020/spark-master:3.0.1-hadoop3.2

USER root

# Install numpy from Alpine repos (pandas will be generated synthetically in code)
RUN apk update && apk add --no-cache py3-numpy

