FROM public.ecr.aws/docker/library/python:3.8 as executor-compile-image

# default values expect that user have cloned core to the Custodian's
# root and execute build with build context pointing to Custodian's root:
# pwd
# ../custodian-as-a-service
# docker build -f executor/Dockerfile .
ARG custodian=.
ARG core=custodian-custom-core
# ARG mcdm=maestro-common-domain-model
# it's not enough to just add a provider here, you must copy its files below
ARG providers="gcp azure"

ARG POETRY_VERSION="1.4.0"
# to import c7n_azure.resources.insights.DiagnosticSettings
ENV AZURE_SUBSCRIPTION_ID=" "

RUN pip install "poetry==$POETRY_VERSION" && \
    python -m venv /root/.local

WORKDIR /build/custodian-custom-core

# Core's root
COPY $core/pyproject.toml $core/poetry.lock $core/README.md ./
RUN . /root/.local/bin/activate && poetry install --no-interaction --without dev --no-root

# Core's providers
COPY $core/tools/c7n_gcp/pyproject.toml $core/tools/c7n_gcp/poetry.lock tools/c7n_gcp/
COPY $core/tools/c7n_azure/pyproject.toml $core/tools/c7n_azure/poetry.lock tools/c7n_azure/
# COPY $core/tools/c7n_azure/pyproject.toml tools/c7n_azure/
RUN for pkg in $providers; do . /root/.local/bin/activate && cd tools/c7n_$pkg && poetry install --no-interaction --without dev --no-root && cd ../..; done

# executor requirements
COPY $custodian/executor/requirements.txt ../executor/requirements.txt
RUN . /root/.local/bin/activate && pip install -r ../executor/requirements.txt


COPY $core/c7n c7n/
# "poetry install --only-root" installs in editable mode
# (like pip install -e .). Due to multi-stage build we do not need this.
# To install in non-editable mode with Poetry you have to build a dist and
# then install which is quite a work so I use "pip install --no-deps ."
# https://github.com/python-poetry/poetry/issues/1382
RUN . /root/.local/bin/activate && pip install --no-deps .

COPY $core/tools/c7n_gcp tools/c7n_gcp/
COPY $core/tools/c7n_azure tools/c7n_azure/
RUN for pkg in $providers; do . /root/.local/bin/activate && cd tools/c7n_$pkg && pip install --no-deps . && cd ../..; done


# COPY $mcdm/mcdm_sdk ../mcdm_sdk
# RUN . /root/.local/bin/activate && pip install ../mcdm_sdk

COPY $custodian/executor ../executor
RUN . /root/.local/bin/activate && python ../executor/helpers/resource_map_generator.py
# by here we have executor built in /root/.local

# 3.8-alpine works as well
FROM public.ecr.aws/docker/library/python:3.8-slim AS build-image

COPY --from=executor-compile-image /root/.local /root/.local
COPY --from=executor-compile-image /build/executor /executor
ENV PATH=/root/.local/bin:$PATH

WORKDIR /executor
