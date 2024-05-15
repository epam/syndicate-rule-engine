FROM public.ecr.aws/docker/library/python:3.10 as executor-compile-image

# default values expect that user have cloned core to the Custodian's
# root and execute build with build context pointing to Custodian's root:
# pwd
# ../custodian-as-a-service
# docker build -f src/executor/Dockerfile .
ARG custodian=.
ARG core=custodian-custom-core
# it's not enough to just add a provider here, you must copy its files below
ARG providers="gcp azure kube"

ARG POETRY_VERSION="1.4.0"
# to import c7n_azure.resources.insights.DiagnosticSettings
ENV AZURE_SUBSCRIPTION_ID=" "

RUN pip install "poetry==$POETRY_VERSION" && python -m venv /root/.local

WORKDIR /build/custodian-custom-core

# Core's root
COPY $core/pyproject.toml $core/poetry.lock $core/README.md ./
RUN . /root/.local/bin/activate && poetry install --no-interaction --without dev --no-root

# Core's providers
COPY $core/tools/c7n_gcp/pyproject.toml $core/tools/c7n_gcp/poetry.lock tools/c7n_gcp/
COPY $core/tools/c7n_azure/pyproject.toml $core/tools/c7n_azure/poetry.lock tools/c7n_azure/
COPY $core/tools/c7n_kube/pyproject.toml $core/tools/c7n_kube/poetry.lock tools/c7n_kube/

RUN for pkg in $providers; do . /root/.local/bin/activate && cd tools/c7n_$pkg && poetry install --no-interaction --without dev --no-root && cd ../..; done

# server requirements
COPY $custodian/src/onprem/requirements.txt ../src/onprem/requirements.txt
RUN . /root/.local/bin/activate && pip install -r ../src/onprem/requirements.txt

# executor requirements
COPY $custodian/src/executor/requirements.txt ../src/executor/requirements.txt
RUN . /root/.local/bin/activate && pip install -r ../src/executor/requirements.txt


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


COPY $custodian/src ../src
# by here we have executor and api built in /root/.local


FROM public.ecr.aws/docker/library/python:3.10-slim AS build-image

COPY --from=executor-compile-image /root/.local /root/.local
COPY --from=executor-compile-image /build/src/ /src/


ENV AWS_REGION=eu-central-1 \
    CAAS_SERVICE_MODE=docker \
    PATH=/root/.local/bin:$PATH \
    modular_service_mode=docker

WORKDIR /src
EXPOSE 8000
RUN chmod +x onprem/scripts/entrypoint.sh main.py
ENTRYPOINT ["onprem/scripts/entrypoint.sh"]
