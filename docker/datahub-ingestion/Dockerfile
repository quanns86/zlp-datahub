# Defining environment
ARG APP_ENV=full
ARG BASE_IMAGE=acryldata/datahub-ingestion-base
ARG DOCKER_VERSION=head

FROM $BASE_IMAGE:$DOCKER_VERSION as base
USER 0

COPY ./metadata-ingestion /datahub-ingestion
COPY ./metadata-ingestion-modules/airflow-plugin /datahub-ingestion/airflow-plugin



ARG RELEASE_VERSION
WORKDIR /datahub-ingestion
RUN sed -i.bak "s/__version__ = \"1!0.0.0.dev0\"/__version__ = \"$RELEASE_VERSION\"/" src/datahub/__init__.py && \
    sed -i.bak "s/__version__ = \"1!0.0.0.dev0\"/__version__ = \"$RELEASE_VERSION\"/" airflow-plugin/src/datahub_airflow_plugin/__init__.py && \
    cat src/datahub/__init__.py && \
    chown -R datahub /datahub-ingestion

USER datahub
ENV PATH="/datahub-ingestion/.local/bin:$PATH"

FROM base as base-miniconda

RUN wget https://repo.anaconda.com/miniconda/Miniconda3-latest-Linux-x86_64.sh -O /tmp/miniconda.sh && \
    bash /tmp/miniconda.sh -b -p /opt/miniconda && \
    rm /tmp/miniconda.sh
ENV PATH=/opt/miniconda/bin:$PATH
# Create python3.7 env using miniconda
RUN conda create --name py37 python=3.7 -y \
    && conda activate py37

FROM base-miniconda as slim-install
RUN conda activate py37 && \
    pip install --no-cache --user ".[base,datahub-rest,datahub-kafka,snowflake,bigquery,redshift,mysql,postgres,hive,clickhouse,glue,dbt,looker,lookml,tableau,powerbi,superset,datahub-business-glossary]"

FROM base-miniconda as full-install
RUN conda activate py37 && \
    pip install --no-cache --user ".[base]" && \
    pip install --no-cache --user "./airflow-plugin[acryl-datahub-airflow-plugin]" && \
    pip install --no-cache --user ".[all]"

FROM base-miniconda as dev-install
# Dummy stage for development. Assumes code is built on your machine and mounted to this image.
# See this excellent thread https://github.com/docker/cli/issues/1134

FROM ${APP_ENV}-install as final

USER datahub
ENV PATH="/datahub-ingestion/.local/bin:$PATH"
