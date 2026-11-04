# syntax=docker/dockerfile:1
# Citus built from source: upstream images/packages are amd64-only, and running
# them under emulation on Apple Silicon would distort every perf measurement.
# The source is our fork (engine/citus submodule), which adds the citus_observer
# instrumentation. Build context is the repository root.
ARG PG_IMAGE=postgres:18.4

FROM ${PG_IMAGE} AS toolchain
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
         build-essential ca-certificates ccache autoconf flex bison \
         "postgresql-server-dev-${PG_MAJOR}" \
         liblz4-dev libzstd-dev libssl-dev libkrb5-dev libicu-dev libselinux1-dev \
         libxslt1-dev libpam0g-dev libreadline-dev zlib1g-dev libcurl4-openssl-dev
ENV PATH=/usr/lib/ccache:$PATH CCACHE_DIR=/ccache

FROM toolchain AS build
COPY engine/citus /src/citus
WORKDIR /src/citus
# ccache persists across builds, so only changed files recompile
RUN --mount=type=cache,target=/ccache \
    ./configure \
    && make -j"$(nproc)" \
    && make install DESTDIR=/out

FROM ${PG_IMAGE}
RUN apt-get update \
    && apt-get install -y --no-install-recommends libcurl4t64 \
    && rm -rf /var/lib/apt/lists/*
COPY --from=build /out/ /
COPY infra/citus/postgresql.citus.conf /etc/postgresql/postgresql.citus.conf
RUN echo "include = '/etc/postgresql/postgresql.citus.conf'" >> /usr/share/postgresql/postgresql.conf.sample
COPY infra/citus/initdb/ /docker-entrypoint-initdb.d/
COPY infra/citus/replica-entrypoint.sh /usr/local/bin/replica-entrypoint.sh
RUN chmod +x /usr/local/bin/replica-entrypoint.sh /docker-entrypoint-initdb.d/*.sh
