FROM userfeeds/base-python-image:latest
ENV COMMAND ./start.py

RUN apk add --no-cache \
    python3-dev \
    postgresql-dev \
    bash

RUN pip install psycopg2
RUN pip install neo4j-driver
RUN pip install retry
RUN pip install PyFunctional

COPY app /app

WORKDIR /app/src/
RUN python3 tests

CMD $COMMAND
