FROM public.ecr.aws/docker/library/python:3.10-alpine

RUN pip install --no-cache-dir modular-sdk==6.0.0

ENV modular_service_mode=docker

COPY --chmod=711 main.py /usr/local/bin/srepatch
ENTRYPOINT ["srepatch"]

