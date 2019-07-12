FROM golang:1.12.3 AS BUILD

RUN apt-get update && apt-get install -y libgeos-dev

RUN mkdir /schelly
WORKDIR /schelly

ADD go.mod .
ADD go.sum .
RUN go mod download

#now build source code
ADD . ./
RUN go build -o /go/bin/schelly



FROM golang:1.12.3

VOLUME [ "/var/lib/schelly/data" ]

ENV BACKUP_NAME             ''
ENV BACKUP_CRON_STRING      ''
ENV RETENTION_CRON_STRING   ''
ENV CONDUCTOR_API_URL       ''
ENV WEBHOOK_GRACE_TIME      3600
ENV DATA_DIR                '/var/lib/schelly/data'

COPY --from=BUILD /go/bin/* /bin/
ADD startup.sh /

CMD [ "/startup.sh" ]


# FROM BUILD AS TEST
# RUN go test -v schelly
