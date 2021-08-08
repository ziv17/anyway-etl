# Pulled August 8, 2021
FROM python:3.8@sha256:caa7d8d6bfaa181f30c5a5074b81b6963246615f0140dca1d86e1e98efa99dc6
RUN pip install --upgrade pip
WORKDIR /srv
COPY requirements.txt ./
RUN pip install -r requirements.txt
# TODO: when https://github.com/hasadna/anyway/pull/1851 is merged, change to dev
ARG ANYWAY_COMMIT=9a8c8f72efe9d1369be096d67978a03fd132cfa1
RUN pip install -e git+https://github.com/hasadna/anyway@${ANYWAY_COMMIT}#egg=anyway
COPY static_data ./static_data
COPY setup.py ./setup.py
COPY anyway_etl ./anyway_etl
RUN pip install -e .
ENV PYTHONUNBUFFERED=1
ENV ANYWAY_ETL_DATA_ROOT_PATH=/var/anyway-etl-data
ENV ANYWAY_ETL_STATIC_DATA_ROOT_PATH=/srv/static_data
ENV SQLALCHEMY_URL=postgresql://anyway:anyway@db/anyway
ENTRYPOINT ["anyway-etl"]
CMD ["--help"]
