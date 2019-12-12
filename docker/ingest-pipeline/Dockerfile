# Parent image
FROM puckel/docker-airflow

LABEL description="HuBMAP Ingest Pipeline" \
	version="0.1"

# need git to install other hubmap requirements
USER root
RUN apt-get update && \
    apt-get upgrade -y && \
    apt-get install -y git
USER airflow

# Airflow API, including hubmap plugin
EXPOSE 8789

# Flower API
EXPOSE 5555