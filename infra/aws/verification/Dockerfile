FROM ubuntu:24.04@sha256:9c7f200c07e3d4a03e7249d2c16bf66a51120fb876aa2242cf31451412543289 AS runtime

# systemd is a probe prerequisite, not a running PID 1 inside this container.
RUN apt-get -o Acquire::Retries=3 update && \
    DEBIAN_FRONTEND=noninteractive apt-get -o Acquire::Retries=3 install -y \
    --no-install-recommends systemd
COPY bootstrap.sh install-bootstrap.sh /opt/probe/
RUN bash /opt/probe/install-bootstrap.sh

WORKDIR /opt/project
COPY requirements.txt pyproject.toml ./
COPY src/ src/
RUN python3.12 -m venv .venv && \
    .venv/bin/python -m pip install -r requirements.txt && \
    .venv/bin/python -m pip install --no-deps -e . && \
    .venv/bin/python -m pip check
ENV SPARK_LOCAL_IP=127.0.0.1

FROM runtime AS bootstrap-probe
COPY probe.py /opt/probe/
CMD [".venv/bin/python", "/opt/probe/probe.py"]

FROM runtime AS runtime-sizing
COPY --from=apache/kafka:4.2.1@sha256:9916d60eca5d599550e2c320230808fda342124ba550bb4ac4ea8591803262a0 /opt/kafka/ /opt/kafka/
COPY config/ config/
COPY runtime_sizing.py /opt/probe/
CMD [".venv/bin/python", "/opt/probe/runtime_sizing.py"]

FROM runtime-sizing AS runtime-restart
COPY runtime_restart.py /opt/probe/
CMD [".venv/bin/python", "/opt/probe/runtime_restart.py"]
