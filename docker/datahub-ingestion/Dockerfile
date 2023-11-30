# Defining environment
ARG APP_ENV=full
ARG BASE_IMAGE=acryldata/datahub-ingestion-base
ARG DOCKER_VERSION=head


FROM continuumio/miniconda3:latest as base-miniconda
RUN conda create -y -n py37 python=3.7 && \
    conda clean -ya

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

COPY --from=base-miniconda /opt/conda /opt/conda

USER datahub
ENV PATH="/opt/conda/bin:$PATH"

# Create python3.7 env using miniconda

FROM base as slim-install
RUN pip install --no-cache --user ".[base,datahub-rest,datahub-kafka,snowflake,bigquery,redshift,mysql,postgres,hive,clickhouse,glue,dbt,looker,lookml,tableau,powerbi,superset,datahub-business-glossary]"

FROM base as full-install
RUN eval "$(conda shell.bash hook)" && \
    conda activate py37 && \ 
    python3 -m pip install --no-cache --user ".[base]" && \
    python3 -m pip install --no-cache --user "./airflow-plugin[acryl-datahub-airflow-plugin]" && \
    python3 -m pip install --no-cache --user ".[all]"

FROM base-miniconda as dev-install
# Dummy stage for development. Assumes code is built on your machine and mounted to this image.
# See this excellent thread https://github.com/docker/cli/issues/1134

FROM ${APP_ENV}-install as final

USER datahub
ENV PATH="/datahub-ingestion/.local/bin:$PATH"
