# Container for a simple subscriber script to demonstrate the system is working and print queued messages

FROM python:2.7-stretch

COPY requirements.txt /usr/src/app/requirements.txt
COPY docker/subscriber/subscribe.py /usr/src/app
COPY config-example.cfg /usr/src/app/config.cfg

RUN pip install --upgrade pip && pip install --no-cache-dir -r /usr/src/app/requirements.txt

CMD ["python", "/usr/src/app/subscribe.py"]
