FROM python:3.12-slim

WORKDIR /app

RUN pip install --no-cache-dir grpcio==1.76.0 protobuf==6.33.0

COPY gen/proto/saga.pb /app/saga.pb
COPY testsuite/step_executor/server.py /app/server.py

ENV STEP_EXECUTOR_DESCRIPTOR=/app/saga.pb

CMD ["python", "/app/server.py"]
