FROM public.ecr.aws/docker/library/python:3.10 as compile-image

# default values expect that user has cloned core to the Custodian's
# root and execute build with build context pointing to Custodian's root:
# pwd
# ../custodian-as-a-service
# docker build -f src/executor/Dockerfile .
ARG CUSTODIAN_SERVICE_PATH=.
ARG CLOUD_CUSTODIAN_PATH=cloudcustodian-sre-core
# it's not enough to just add a provider here, you must copy its files below
ARG PROVIDERS="gcp azure kube"

ARG POETRY_VERSION="1.8.3"

RUN pip install "poetry==$POETRY_VERSION" && python -m venv /root/.local

WORKDIR /build/cloudcustodian-sre-core

# Core's root
COPY $CLOUD_CUSTODIAN_PATH/pyproject.toml $CLOUD_CUSTODIAN_PATH/poetry.lock $CLOUD_CUSTODIAN_PATH/README.md ./
RUN . /root/.local/bin/activate && poetry install --no-interaction --without dev --no-root

# [FROM Cloud Custodian's Dockerfile]:
# Now install the root package, we used to do this after dependencies of other providers
# but since moving c7n to a main dependency in pyproject toml we have to do this one first.
COPY $CLOUD_CUSTODIAN_PATH/c7n c7n/
# "poetry install --only-root" installs in editable mode
# (like pip install -e .). Due to multi-stage build we do not need this.
# To install in non-editable mode with Poetry you have to build a dist and
# then install which is quite a work so I use "pip install --no-deps ."
# https://github.com/python-poetry/poetry/issues/1382
RUN . /root/.local/bin/activate && pip install --no-deps .

# Core's providers
COPY $CLOUD_CUSTODIAN_PATH/tools/c7n_gcp/pyproject.toml $CLOUD_CUSTODIAN_PATH/tools/c7n_gcp/poetry.lock tools/c7n_gcp/
COPY $CLOUD_CUSTODIAN_PATH/tools/c7n_azure/pyproject.toml $CLOUD_CUSTODIAN_PATH/tools/c7n_azure/poetry.lock tools/c7n_azure/
COPY $CLOUD_CUSTODIAN_PATH/tools/c7n_kube/pyproject.toml $CLOUD_CUSTODIAN_PATH/tools/c7n_kube/poetry.lock tools/c7n_kube/

RUN for pkg in $PROVIDERS; do . /root/.local/bin/activate && cd tools/c7n_$pkg && poetry install --no-interaction --without dev --no-root && cd ../..; done

# server requirements
COPY $CUSTODIAN_SERVICE_PATH/src/onprem/requirements.txt ../src/onprem/requirements.txt
RUN . /root/.local/bin/activate && pip install -r ../src/onprem/requirements.txt


COPY $CLOUD_CUSTODIAN_PATH/tools/c7n_gcp tools/c7n_gcp/
COPY $CLOUD_CUSTODIAN_PATH/tools/c7n_azure tools/c7n_azure/
COPY $CLOUD_CUSTODIAN_PATH/tools/c7n_kube tools/c7n_kube/
RUN for pkg in $PROVIDERS; do . /root/.local/bin/activate && cd tools/c7n_$pkg && pip install --no-deps . && cd ../..; done


COPY $CUSTODIAN_SERVICE_PATH/src ../src
# by here we have executor and api built in /root/.local


FROM public.ecr.aws/docker/library/python:3.10-slim AS build-image

COPY --from=compile-image /root/.local /root/.local
COPY --from=compile-image /build/src/ /src/


ENV AWS_REGION=us-east-1 \
    CAAS_SERVICE_MODE=docker \
    PATH=/root/.local/bin:$PATH \
    modular_service_mode=docker

WORKDIR /src
EXPOSE 8000
RUN chmod +x entrypoint.sh main.py
ENTRYPOINT ["/src/entrypoint.sh"]
