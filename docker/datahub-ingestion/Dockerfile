# Defining environment
ARG APP_ENV=prod

FROM acryldata/datahub-ingestion-base as base

FROM openjdk:11 as prod-build
COPY . /datahub-src
RUN cd /datahub-src && ./gradlew :metadata-events:mxe-schemas:build

FROM base as prod-codegen
COPY --from=prod-build /datahub-src /datahub-src
RUN cd /datahub-src/metadata-ingestion && \
    pip install -e ".[base]" && \
    ./scripts/codegen.sh

FROM ubuntu:head as miniconda-build
RUN curl https://repo.anaconda.com/miniconda/Miniconda3-py39_4.12.0-Linux-x86_64.sh > /miniconda.sh && \
    bash /miniconda.sh -b -p /miniconda

FROM base as prod-install
ARG RELEASE_VERSION
COPY --from=prod-codegen /datahub-src/metadata-ingestion /datahub-ingestion
COPY --from=prod-codegen /root/.cache/pip /root/.cache/pip
COPY --from=miniconda-build /miniconda /miniconda
RUN cd /datahub-ingestion && \
    sed -i.bak "s/__version__ = \"0.0.0.dev0\"/__version__ = \"$RELEASE_VERSION\"/" src/datahub/__init__.py && \
    cat src/datahub/__init__.py && \
    pip install ".[all]" && \
    pip freeze

FROM ${APP_ENV}-install as final
RUN addgroup --system datahub && adduser --system datahub --ingroup datahub
USER datahub

ENTRYPOINT [ "datahub" ]