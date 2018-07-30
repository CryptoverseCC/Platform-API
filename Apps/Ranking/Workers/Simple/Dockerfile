FROM userfeeds/base-python-image:latest

RUN apk add --no-cache python3-dev postgresql-dev bash

EXPOSE 8000

RUN pip install psycopg2
RUN pip install neo4j-driver
RUN pip install retry
RUN pip install PyFunctional
RUN pip install --no-cache-dir flask-cors && rm -rf /root/.cache

COPY app /app

WORKDIR /app/src/
RUN python3 tests

CMD /usr/local/bin/gunicorn --log-config /app/logging.conf -w 16 -b :8000 start:app
