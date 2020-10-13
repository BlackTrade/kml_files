FROM python:3.7.5

# Change TimeZone for correct time
RUN ln -snf /usr/share/zoneinfo/Europe/Moscow /etc/localtime

ARG URL_PATH="input_path"


# install libaio
RUN curl $URL_PATH/binaries/deb/libaio1_0.3.112-5_amd64.deb > /tmp/libaio1_0.3.112-5_amd64.deb \
    && dpkg -i /tmp/libaio1_0.3.112-5_amd64.deb && rm -rf /tmp/libaio1_0.3.112-5_amd64.deb

# Install Oracle instant client
WORKDIR /opt
RUN curl $URL_PATH/OIC/instantclient_18_3.tgz | tar -xz \
    && echo /opt/instantclient_18_3 > /etc/ld.so.conf.d/oracle-instantclient.conf \
    && ldconfig

WORKDIR /d/app_data
RUN mkdir -p requirements kml_files TNS log_json \
    && mkdir -p kml_files/export_kml

# set up ORCALE-CLIENT
RUN echo "DWX=(DESCRIPTION=(ADDRESS=(PROTOCOL=tcp)(HOST=dwx.megafon.ru)(PORT=1521))(CONNECT_DATA=(SID=bd_dwx)(SERVICE_NAME=bd_dwx)))" > /d/app_data/TNS/tnsnames.ora \
    && echo "dwx_stb=(DESCRIPTION=(ADDRESS = (PROTOCOL = TCP)(HOST = dwx-standby.megafon.ru)(PORT = 1521))(CONNECT_DATA = (SERVER = DEDICATED)(SERVICE_NAME = bo_dwx) ))" >> /d/app_data/TNS/tnsnames.ora
ENV ORACLE_HOME="/opt/instantclient_18_3"
ENV TNS_ADMIN="/d/app_data/TNS"
ENV LC_ALL="en_US.UTF-8"

# instal libs
RUN LIB_PATH="/usr/local/lib/libspatialindex-1.9.3" \
    && mkdir $LIB_PATH && cd $LIB_PATH \
    && curl $URL_PATH/binaries/python3/libspatialindex-1.9.3-he1b5a44_1.tar.bz2 | tar -xj \
    && echo "${LIB_PATH}/lib" > /etc/ld.so.conf.d/libspatialindex.conf && ldconfig

COPY ./requirements.txt /d/app_data/requirements
COPY ./log_main.json /d/app_data/log_json
COPY ./kml_files/ /d/app_data/kml_files


RUN export HTTPS_PROXY="https://msk-proxy.megafon.ru:3128" \
    && pip3 install -r /d/app_data/requirements/requirements.txt

WORKDIR /d/app_data/kml_files

ENTRYPOINT [ "bash", "./run.sh" ]
