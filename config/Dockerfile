FROM apache/airflow:2.9.1-python3.11

USER root

# Install ant
RUN apt update && \
    apt-get install -y ant && \
    apt-get clean;

# Downloading gcloud package
RUN  curl https://dl.google.com/dl/cloudsdk/release/google-cloud-sdk.tar.gz > /tmp/google-cloud-sdk.tar.gz
# Installing the package
RUN mkdir -p /usr/local/gcloud \
    && tar -C /usr/local/gcloud -xvf /tmp/google-cloud-sdk.tar.gz \
    && /usr/local/gcloud/google-cloud-sdk/install.sh

# Adding the package path to local
ENV PATH $PATH:/usr/local/gcloud/google-cloud-sdk/bin


USER airflow

COPY requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt
RUN rm requirements.txt