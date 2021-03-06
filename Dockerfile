FROM python:3.6
MAINTAINER James Endicott <james.endicott@colorado.edu>
WORKDIR /app
ENTRYPOINT ["/bin/bash", "-c", "source /app/sh/entrypoint.sh"]

RUN echo "deb http://http.debian.net/debian jessie-backports main" > /etc/apt/sources.list.d/jessie-backports.list \
    && apt-get update \
    && apt-get -y install wget ant \
    && apt-get install -t jessie-backports -y openjdk-8-jdk openjdk-8-jre-headless \
    && /usr/sbin/update-java-alternatives -s java-1.8.0-openjdk-amd64 \
    && wget https://github.com/mimno/Mallet/archive/master.tar.gz \
    && tar -xzvf master.tar.gz \
    && rm master.tar.gz \
    && mv Mallet-master Mallet \
    && cd Mallet \
    && ant \
    && apt-get purge --auto-remove -y wget ant openjdk-8-jdk \
    && rm -rf /var/lib/apt/lists/* \
    && pip install \
        pandas \
        pyyaml \
        scipy \
        sklearn

COPY ./ /app/