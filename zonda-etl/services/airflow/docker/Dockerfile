FROM puckel/docker-airflow
MAINTAINER "Mauro Rodriguez"

# define proxy
ENV http_proxy http://proxy.ar.bsch:8080
ENV https_proxy https://proxy.ar.bsch:8080

# install python dependencies

USER root
RUN printf "deb http://archive.debian.org/debian/ jessie main\ndeb-src http://archive.debian.org/debian/ jessie main\ndeb http://security.debian.org jessie/updates main\ndeb-src http://security.debian.org jessie/updates main" > /etc/apt/sources.list
RUN apt-get update
RUN apt-get install -y libsasl2-modules
RUN apt-get install -y --allow-downgrades libsasl2-2=2.1.26.dfsg1-13+deb8u1
RUN apt-get install -y libsasl2-dev
RUN apt-get install -y wget
RUN wget https://download.java.net/java/GA/jdk12.0.2/e482c34c86bd4bf8b56c0b35558996b9/10/GPL/openjdk-12.0.2_linux-x64_bin.tar.gz
RUN tar -xvzf openjdk-12.0.2_linux-x64_bin.tar.gz
RUN apt-get install sudo
RUN sudo update-alternatives --install "/usr/bin/java" "java" "/usr/local/airflow/jdk-12.0.2/bin/java" 1500

RUN java --version

COPY requirements.txt .
RUN pip3 install -r $PWD/requirements.txt
RUN pip3 install websocket
RUN pip3 install websocket-client

RUN pip list
# ui customization
COPY ui/1.10.3/custom_trigger.html /usr/local/lib/python3.6/site-packages/airflow/www/templates/airflow/
COPY ui/1.10.3/dag.html /usr/local/lib/python3.6/site-packages/airflow/www/templates/airflow/
COPY ui/1.10.3/dags.html /usr/local/lib/python3.6/site-packages/airflow/www/templates/airflow/
COPY ui/1.10.3/views.py /usr/local/lib/python3.6/site-packages/airflow/www/

