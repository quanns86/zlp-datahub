# Defining environment
ARG APP_ENV=prod
ARG DOCKER_VERSION=latest
FROM acryldata/datahub-actions:head as base

FROM eclipse-temurin:11 as prod-build
COPY . /datahub-src
WORKDIR /datahub-src
# We noticed that the gradle wrapper download failed frequently on in CI on arm64 machines.
# I suspect this was due because of the QEMU emulation slowdown, combined with the arm64
# build being starved for CPU by the x86_64 build's codegen step.
#
# The middle step will attempt to download gradle wrapper 5 times with exponential backoff.
# The ./gradlew --version will force the download of the gradle wrapper but is otherwise a no-op.
# Note that the retry logic will always return success, so we should always attempt to run codegen.
# Inspired by https://github.com/gradle/gradle/issues/18124#issuecomment-958182335.
# and https://unix.stackexchange.com/a/82610/378179.
# This is a workaround for https://github.com/gradle/gradle/issues/18124.
RUN (for attempt in 1 2 3 4 5; do ./gradlew --version && break ; echo "Failed to download gradle wrapper (attempt $attempt)" && sleep $((2<<$attempt)) ; done ) && \
    ./gradlew :metadata-events:mxe-schemas:build

FROM base as prod-codegen
USER root
COPY --from=prod-build /datahub-src /datahub-src
RUN cd /datahub-src/metadata-ingestion && \
    pip install -e ".[base]" && \
    ./scripts/codegen.sh

# Build actions
FROM base as prod-install
USER root
COPY --from=prod-codegen /datahub-src/metadata-ingestion /datahub-ingestion
COPY --from=prod-codegen /root/.cache/pip /root/.cache/pip
ARG RELEASE_VERSION
RUN cd /datahub-ingestion && \
    sed -i.bak "s/__version__ = \"0.0.0.dev0\"/__version__ = \"$RELEASE_VERSION\"/" src/datahub/__init__.py && \
    cat src/datahub/__init__.py && \
    pip install ".[all]" && \
    pip freeze && \
    # This is required to fix security vulnerability in htrace-core4
    rm -f /usr/local/lib/python3.10/site-packages/pyspark/jars/htrace-core4-4.1.0-incubating.jar

FROM ${APP_ENV}-install as final
RUN addgroup --system datahub && adduser --system datahub --ingroup datahub
USER datahub
RUN curl -s "https://get.sdkman.io" | bash
RUN /bin/bash -c "source /$HOME/.sdkman/bin/sdkman-init.sh; sdk version; sdk install java 8.0.332-zulu"

CMD dockerize -wait ${DATAHUB_GMS_PROTOCOL:-http}://$DATAHUB_GMS_HOST:$DATAHUB_GMS_PORT/health -timeout 240s /start_datahub_actions.sh