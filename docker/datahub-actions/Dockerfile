# Copyright 2021 Acryl Data, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# Defining environment
ARG APP_ENV=prod

# Build metadata-ingestion
FROM acryldata/datahub-ingestion-base as base
RUN curl https://repo.anaconda.com/miniconda/Miniconda3-py39_4.12.0-Linux-x86_64.sh > /miniconda.sh && \
    bash /miniconda.sh -b -p /miniconda && \
    eval "$(/miniconda/bin/conda shell.bash hook)" && \
    python3 -m pip install -r requirements.txt && \
    conda init && \
    conda activate base

FROM openjdk:11 as prod-ingestion-build
COPY . /datahub-src
RUN cd /datahub-src && ./gradlew :metadata-events:mxe-schemas:build

FROM base as prod-ingestion-codegen
COPY --from=prod-ingestion-build /datahub-src /datahub-src
RUN ls
RUN cd /datahub-src/metadata-ingestion && \
    pip install -e ".[base]" && \
    ./scripts/codegen.sh

# Build actions
FROM acryldata/datahub-ingestion-base:latest as prod-install
COPY /datahub-actions /actions-src
RUN mkdir -p /etc/datahub/actions && mkdir -p /tmp/datahub/logs/actions/system
RUN cd /actions-src && \
    pip install --no-binary :all: "confluent_kafka==${CONFLUENT_KAFKA_VERSION}" && \
    pip install "." && \
    pip install '.[executor]'

COPY --from=prod-ingestion-codegen /datahub-src/metadata-ingestion /datahub-ingestion
COPY --from=prod-ingestion-codegen /root/.cache/pip /root/.cache/pip
RUN cd /datahub-ingestion && \
    sed -i.bak "s/__version__ = \"0.0.0.dev0\"/__version__ = \"$RELEASE_VERSION\"/" src/datahub/__init__.py && \
    cat src/datahub/__init__.py && \
    pip install ".[all]" && \
    pip freeze
# Pyspark 2.4.5
COPY --from=prod-ingestion-codegen /datahub-src/datahub-actions/cloudpickle.py /usr/local/lib/python3.9/site-packages/pyspark/cloudpickle.py
COPY --from=prod-ingestion-codegen /datahub-src/datahub-actions/run_ingest.sh /usr/local/bin/run_ingest.sh
COPY ./docker/datahub-actions/start.sh /start_datahub_actions.sh
RUN chmod a+x /start_datahub_actions.sh

# Add other default configurations into this!
RUN mkdir -p /etc/datahub/actions/conf && mkdir -p /etc/datahub/actions/system/conf
COPY ./docker/datahub-actions/config/executor.yaml /etc/datahub/actions/system/conf

RUN addgroup --system datahub && adduser --system datahub --ingroup datahub \
    && chown datahub /etc/datahub \
    && chown -R datahub /tmp/datahub

# By transferring the root user's pip cache directory to the datahub
# user, we can avoid the need for some redundant dependency downloads.
RUN mkdir -p /home/datahub/.cache \
    && mv /root/.cache/pip /home/datahub/.cache/pip \
    && chown -R datahub /home/datahub/.cache/pip



FROM ${APP_ENV}-install as final
USER datahub
RUN curl -s "https://get.sdkman.io" | bash
RUN /bin/bash -c "source /$HOME/.sdkman/bin/sdkman-init.sh; sdk version; sdk install java 8.0.332-zulu"
CMD dockerize -wait http://$DATAHUB_GMS_HOST:$DATAHUB_GMS_PORT/health -timeout 240s /start_datahub_actions.sh
