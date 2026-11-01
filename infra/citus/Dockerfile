# Citus built from source: upstream images/packages are amd64-only, and running
# them under emulation on Apple Silicon would distort every perf measurement.
ARG PG_IMAGE=postgres:18.4

FROM ${PG_IMAGE} AS build
ARG CITUS_VERSION=v14.2.0
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
         build-essential ca-certificates git autoconf flex bison \
         "postgresql-server-dev-${PG_MAJOR}" \
         liblz4-dev libzstd-dev libssl-dev libkrb5-dev libicu-dev libselinux1-dev \
         libxslt1-dev libpam0g-dev libreadline-dev zlib1g-dev libcurl4-openssl-dev
RUN git clone --depth 1 --branch "${CITUS_VERSION}" https://github.com/citusdata/citus.git /src/citus
WORKDIR /src/citus
RUN ./configure \
    && make -j"$(nproc)" \
    && make install DESTDIR=/out

FROM ${PG_IMAGE}
RUN apt-get update \
    && apt-get install -y --no-install-recommends libcurl4t64 \
    && rm -rf /var/lib/apt/lists/*
COPY --from=build /out/ /
COPY postgresql.citus.conf /etc/postgresql/postgresql.citus.conf
RUN echo "include = '/etc/postgresql/postgresql.citus.conf'" >> /usr/share/postgresql/postgresql.conf.sample
COPY initdb/ /docker-entrypoint-initdb.d/
COPY replica-entrypoint.sh /usr/local/bin/replica-entrypoint.sh
RUN chmod +x /usr/local/bin/replica-entrypoint.sh /docker-entrypoint-initdb.d/*.sh
