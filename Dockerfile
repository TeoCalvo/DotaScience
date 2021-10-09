FROM openjdk:8

COPY --from=python:3.9 / /

COPY requirements.txt .

RUN pip install -r requirements.txt

CMD tail -f /dev/null