FROM python:3.12-slim-bullseye AS requirements

WORKDIR /app

ADD Pipfile.lock /app/Pipfile.lock
RUN pip install pipenv
RUN pipenv requirements > requirements.txt

FROM python:3.12-slim-bullseye AS install-requirements

COPY --from=requirements /app/requirements.txt /app/requirements.txt
RUN pip install -r /app/requirements.txt

FROM python:3.12-slim-bullseye

RUN apt-get update && apt-get install -y \
  git \
  && rm -rf /var/lib/apt/lists/*

COPY --from=install-requirements /usr/local/lib/python3.12/site-packages /usr/local/lib/python3.12/site-packages

WORKDIR /app

ADD ./*.py /app

CMD [ "python", "build.py" ]
