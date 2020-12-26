FROM ubuntu:18.04

ARG repository="deb https://sourceforge.net/projects/clickhouse/files/apt/ clickhouse-server main"
ARG version=19.17.6
ARG gosu_ver=1.10

RUN sed -i s@/archive.ubuntu.com/@/mirrors.aliyun.com/@g /etc/apt/sources.list \
    && apt-get update \
    && apt-get install --yes --no-install-recommends \
        apt-transport-https \
        dirmngr \
        gnupg \
        wget \
        ca-certificates \
    && mkdir -p /etc/apt/sources.list.d \
    && wget -O - https://sourceforge.net/projects/clickhouse/files/apt/key/deb.gpg.key | apt-key add - \
    && echo $repository > /etc/apt/sources.list.d/clickhouse.list \
    && apt-get update \
    && env DEBIAN_FRONTEND=noninteractive \
        apt-get install --allow-unauthenticated --yes --no-install-recommends \
            clickhouse-common-static=$version \
            clickhouse-client=$version \
            clickhouse-server=$version \
            libgcc-7-dev \
            locales \
            tzdata \
    && rm -rf \
        /var/lib/apt/lists/* \
        /var/cache/debconf \
        /tmp/* \
    && apt-get clean

ADD https://github.com/tianon/gosu/releases/download/$gosu_ver/gosu-amd64 /bin/gosu

RUN locale-gen en_US.UTF-8
ENV LANG en_US.UTF-8
ENV LANGUAGE en_US:en
ENV LC_ALL en_US.UTF-8

RUN mkdir /docker-entrypoint-initdb.d

COPY docker_related_config.xml /etc/clickhouse-server/config.d/
COPY entrypoint.sh /entrypoint.sh

RUN chmod +x \
    /entrypoint.sh \
    /bin/gosu

EXPOSE 9000 8123 9009
VOLUME /var/lib/clickhouse

ENV CLICKHOUSE_CONFIG /etc/clickhouse-server/config.xml

ENTRYPOINT ["/entrypoint.sh"]

