FROM python:3.11-slim

ENV PIP_NO_CACHE_DIR=off \
    PIP_DISABLE_PIP_VERSION_CHECK=on \
    UV_LINK_MODE=copy

RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
    gnupg \
    wget \
    git \
    cmake \
    pkg-config \
    libopenjp2-7-dev \
    libcharls-dev \
    zlib1g-dev \
  && install -d /usr/share/keyrings \
  && wget -qO - https://www.postgresql.org/media/keys/ACCC4CF8.asc | gpg --dearmor -o /usr/share/keyrings/pgdg-archive-keyring.gpg \
  && echo "deb [signed-by=/usr/share/keyrings/pgdg-archive-keyring.gpg] http://apt.postgresql.org/pub/repos/apt $(. /etc/os-release && echo $VERSION_CODENAME)-pgdg main" > /etc/apt/sources.list.d/pgdg.list \
  && apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    libpq-dev \
    postgresql-client-16 \
    p7zip-full \
    par2 \
  && rm -rf /var/lib/apt/lists/*

WORKDIR /app

RUN pip install --upgrade pip uv

RUN git clone https://github.com/rordenlab/dcm2niix.git /tmp/dcm2niix \
  && mkdir -p /tmp/dcm2niix/build \
  && cd /tmp/dcm2niix/build \
  && cmake -DZLIB_IMPLEMENTATION=Cloudflare -DUSE_JPEGLS=ON -DUSE_OPENJPEG=ON .. \
  && make -j"$(nproc)" \
  && make install \
  && rm -rf /tmp/dcm2niix

COPY pyproject.toml ./
COPY src ./src

RUN uv pip install --system .[dev]

ENTRYPOINT ["uv", "run", "neuro-backend"]
