# Parent image
FROM puckel/docker-airflow

LABEL description="HuBMAP Ingest Pipeline" \
	version="0.1"

# need git to install other hubmap requirements
USER root
RUN apt-get update && \
    apt-get upgrade -y && \
    apt-get install -y git && \
    apt-get install -y sudo
RUN echo 'airflow ALL=(ALL) NOPASSWD: /bin/chown airflow ., /bin/chgrp airflow .' > /etc/sudoers.d/90-airflow
    
USER airflow

