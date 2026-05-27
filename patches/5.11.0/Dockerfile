FROM public.ecr.aws/docker/library/python:3.10-alpine

RUN pip install --no-cache-dir pymongo==4.11.2

COPY --chmod=711 main.py /usr/local/bin/srepatch.py

ENTRYPOINT ["python", "/usr/local/bin/srepatch.py"]