FROM public.ecr.aws/docker/library/python:3.10 as executor-compile-image

# default values expect that user have cloned core to the Custodian's
# root and execute build with build context pointing to Custodian's root:
# pwd
# ../custodian-as-a-service
# docker build -f executor/Dockerfile .
ARG custodian=.
ARG core=custodian-custom-core
# ARG mcdm=maestro-common-domain-model
# it's not enough to just add a provider here, you must copy its files below
ARG providers="gcp azure kube"

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
COPY $core/tools/c7n_kube/pyproject.toml $core/tools/c7n_kube/poetry.lock tools/c7n_kube/

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
COPY $core/tools/c7n_kube tools/c7n_kube/
RUN for pkg in $providers; do . /root/.local/bin/activate && cd tools/c7n_$pkg && pip install --no-deps . && cd ../..; done


# COPY $mcdm/mcdm_sdk ../mcdm_sdk
# RUN . /root/.local/bin/activate && pip install ../mcdm_sdk

COPY $custodian/executor ../executor
# by here we have executor built in /root/.local


# building API
FROM public.ecr.aws/docker/library/python:3.10 as api-compile-image

ARG custodian=.
# ARG mcdm=maestro-common-domain-model

RUN python -m venv /root/.local
WORKDIR /build/custodian-as-a-service

# copying and installing API requirements
COPY $custodian/src/exported_module/requirements.txt src/exported_module/requirements.txt
RUN . /root/.local/bin/activate && pip install -r src/exported_module/requirements.txt

# copying api and mcdm_sdk sources
COPY $custodian/src src
# COPY $mcdm/mcdm_sdk ../mcdm_sdk/

# RUN . /root/.local/bin/activate && pip install ../mcdm_sdk/
# by here api image is built

FROM public.ecr.aws/docker/library/python:3.10-slim AS build-image


COPY --from=api-compile-image /root/.local /root/.local
COPY --from=api-compile-image /build/custodian-as-a-service /custodian-as-a-service

COPY --from=executor-compile-image /build/executor /custodian-as-a-service/executor
COPY --from=executor-compile-image /root/.local /custodian-as-a-service/executor/.executor_venv


ENV AWS_REGION=eu-central-1 \
    SERVICE_MODE=docker \
    VENV_PATH=/custodian-as-a-service/executor/.executor_venv/bin/python \
    EXECUTOR_PATH=/custodian-as-a-service/executor/executor.py \
    PATH=/root/.local/bin:$PATH \
    modular_service_mode=docker

WORKDIR /custodian-as-a-service/src
EXPOSE 8000
RUN chmod +x exported_module/scripts/run.sh
CMD ["exported_module/scripts/run.sh"]
