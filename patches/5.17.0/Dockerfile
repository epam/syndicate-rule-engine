########################################################
# Builder step
########################################################
FROM public.ecr.aws/docker/library/python:3.10 as builder
COPY --from=ghcr.io/astral-sh/uv:0.6.6 /uv /uvx /bin/

ARG PATCH_VERSION=5.17.0

WORKDIR /src

# TODO: use RUN --mount=type=bind for pyproject.toml and uv.lock
COPY pyproject.toml uv.lock /src/

ENV UV_LINK_MODE=copy \
    UV_NO_INSTALLER_METADATA=1 \
    UV_COMPILE_BYTECODE=1
RUN uv sync --frozen --no-install-project --no-editable --group=c7n --compile-bytecode

COPY src /src/
COPY patches/$PATCH_VERSION/main.py /src/main.py
RUN chmod +x /src/main.py && \
    rm -f /src/executor/Dockerfile* /src/executor/requirements.txt /src/onprem/Dockerfile* /src/onprem/requirements.txt /src/uv.lock /src/pyproject.toml

########################################################
# Final step
########################################################
FROM public.ecr.aws/docker/library/python:3.10-slim

ARG USERNAME=sre

RUN groupadd --gid=1337 $USERNAME && \
    useradd --uid=1001 --gid=1337 --no-create-home $USERNAME

COPY --from=builder --chown=$USERNAME:$USERNAME /src /src

WORKDIR /src
USER $USERNAME
ENV AWS_REGION=us-east-1 \
    SRE_SERVICE_MODE=docker \
    MODULAR_SDK_SERVICE_MODE=docker \
    PATH=/src/.venv/bin:$PATH
EXPOSE 8000
ENTRYPOINT ["python", "/src/main.py"]
