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
FROM acryldata/acryl-datahub-actions:head as base

FROM openjdk:11 as prod-ingestion-build
COPY . /datahub-src
RUN cd /datahub-src && ./gradlew :metadata-events:mxe-schemas:build

FROM base as prod-ingestion-codegen
USER root
COPY --from=prod-ingestion-build /datahub-src /datahub-src
RUN chmod -R 777 /datahub-src
RUN cd /datahub-src/metadata-ingestion && \
    pip install -e ".[base]" && \
    ./scripts/codegen.sh

# Build actions
FROM base as prod-install
USER datahub
COPY --from=prod-ingestion-codegen /datahub-src/metadata-ingestion /datahub-ingestion
RUN curl https://repo.anaconda.com/miniconda/Miniconda3-py39_4.12.0-Linux-x86_64.sh > $HOME/miniconda.sh && \
    bash $HOME/miniconda.sh -b && \
    eval "$($HOME/miniconda3/bin/conda shell.bash hook)" && \
    conda init && \
    python3 -m pip install -r requirements.txt && \
    cd /datahub-ingestion && \
    sed -i.bak "s/__version__ = \"0.0.0.dev0\"/__version__ = \"$RELEASE_VERSION\"/" src/datahub/__init__.py && \
    pip install ".[dev]"

# Pyspark 2.4.5
COPY --from=prod-ingestion-codegen /datahub-src/datahub-actions/run_ingest.sh /usr/local/bin/run_ingest.sh

FROM ${APP_ENV}-install as final
USER datahub
CMD dockerize -wait http://$DATAHUB_GMS_HOST:$DATAHUB_GMS_PORT/health -timeout 240s /start_datahub_actions.sh
