# syntax=docker/dockerfile:1
FROM python:3.13-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

COPY .tailnet/vgi-rpc /workspace/vgi-rpc
COPY . /workspace/vgi-python
WORKDIR /workspace/vgi-python
RUN python -m pip install --no-cache-dir "/workspace/vgi-rpc[http]" "/workspace/vgi-python[http]"

CMD ["python", "-m", "tests.tailnet.probe", "--help"]
