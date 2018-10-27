FROM python:2.7.15-alpine3.8

COPY . /usr/src/app

RUN apk add --no-cache bash curl
RUN /bin/bash -c /usr/src/app/setup.sh

CMD ["python", "/usr/src/app/mysql_monitor.py"]
