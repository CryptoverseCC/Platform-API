FROM userfeeds/base-python-image

RUN apk add --no-cache python3-dev postgresql-dev

RUN pip install psycopg2
RUN pip install neo4j-driver
RUN pip install retry

COPY app /app

WORKDIR /app/src/
RUN python3 tests

CMD ./start.py
