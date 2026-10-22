FROM python:3.11-slim-bookworm

WORKDIR /client
COPY docker/client-requirements.txt ./
RUN pip install --no-cache-dir -r client-requirements.txt
COPY proto/reference.pb ./reference.pb
COPY docker/client.py ./client.py
COPY tests/backend_modes_client.py ./backend_modes_client.py

ENTRYPOINT ["python", "/client/client.py"]
