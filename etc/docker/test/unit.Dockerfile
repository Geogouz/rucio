ARG PYTHON=3.9

FROM python:${PYTHON}-slim-bookworm
    WORKDIR /rucio_source

    RUN apt-get update && \
        apt-get install -y --no-install-recommends \
        gcc \
        git \
        libkrb5-dev \
        libmagic1 \
        libxmlsec1 \
        libxmlsec1-dev \
        pkg-config && \
        rm -rf /var/lib/apt/lists/*

    COPY requirements/requirements.dev.txt requirements/requirements.devenv.txt /tmp/requirements/
    RUN python -m pip --no-cache-dir install --upgrade pip setuptools wheel && \
        python -m pip --no-cache-dir install \
            -r /tmp/requirements/requirements.dev.txt \
            -r /tmp/requirements/requirements.devenv.txt

    ENV PYTEST_DISABLE_PLUGIN_AUTOLOAD=true
    ENTRYPOINT ["python", "-bb", "-m", "pytest"]
