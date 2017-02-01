FROM java:8

MAINTAINER boyarsky.vitaliy@live.com

ARG VERSION=1.7.0
ARG MIRROR='http://www.apache.org/dist'

RUN curl -L ${MIRROR}/flume/${VERSION}/apache-flume-${VERSION}-bin.tar.gz -o /tmp/apache-flume-${VERSION}-bin.tar.gz \
    && echo $(curl -s ${MIRROR}/flume/${VERSION}/apache-flume-${VERSION}-bin.tar.gz.md5) /tmp/apache-flume-${VERSION}-bin.tar.gz \
        > /tmp/apache-flume-${VERSION}-bin.tar.gz.md5 \
    && md5sum -c /tmp/apache-flume-${VERSION}-bin.tar.gz.md5

RUN tar xvzf /tmp/apache-flume-${VERSION}-bin.tar.gz -C /opt \
    && mv /opt/apache-flume-${VERSION}-bin /opt/flume \
    && rm -f /tmp/apache-flume-*

ENV PATH=/opt/flume/bin:${PATH}

WORKDIR /opt/flume

ENTRYPOINT ["bin/flume-ng", "agent"]