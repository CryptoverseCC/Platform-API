FROM userfeeds/base-python-image:latest

RUN apk add --no-cache python3-dev postgresql-dev bash
RUN apk add --no-cache curl

EXPOSE 8000

RUN pip install psycopg2
RUN pip install neo4j-driver
RUN pip install retry
RUN pip install PyFunctional
RUN pip install --no-cache-dir flask-cors && rm -rf /root/.cache
RUN pip install json-sempai
RUN pip install web3
RUN pip install cachetools

COPY app /app

WORKDIR /app/src/
RUN curl https://raw.githubusercontent.com/CryptoverseCC/contract-mapping/master/mapping.json > mapping.json
RUN python3 tests

CMD /usr/local/bin/gunicorn --log-config /app/logging.conf -w 16 -b :8000 start:app
