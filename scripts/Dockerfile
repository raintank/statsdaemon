FROM alpine
MAINTAINER Anthony Woods awoods@raintank.io

RUN apk --update add ca-certificates
COPY build/statsdaemon.ini /etc/statsdaemon.ini

COPY build/statsdaemon /usr/bin/statsdaemon
COPY entrypoint.sh /usr/bin/

EXPOSE 8125
EXPOSE 8126

ENTRYPOINT ["/usr/bin/entrypoint.sh"]
