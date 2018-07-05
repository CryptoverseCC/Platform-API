FROM userfeeds/base-python-image:latest

COPY app /app

EXPOSE 8000

WORKDIR /app/src

RUN pip install --no-cache-dir flask-cors && rm -rf /root/.cache
RUN python -V
RUN python tests.py

CMD /usr/local/bin/gunicorn --log-config /app/logging.conf -w 8 -b :8000 start:app
