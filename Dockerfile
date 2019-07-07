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
ENV WEBHOOK_URL             ''
ENV WEBHOOK_HEADERS         ''
ENV WEBHOOK_CREATE_BODY     ''
ENV WEBHOOK_DELETE_BODY     ''
ENV WEBHOOK_GRACE_TIME      3600
ENV DATA_DIR                '/var/lib/schelly/data'

ENV RETENTION_MINUTELY    0@L
ENV RETENTION_HOURLY      0@L
ENV RETENTION_DAILY       4@L
ENV RETENTION_WEEKLY      4@L
ENV RETENTION_MONTHLY     3@L
ENV RETENTION_YEARLY      2@L

COPY --from=BUILD /go/bin/* /bin/
ADD startup.sh /

CMD [ "/startup.sh" ]


# FROM BUILD AS TEST
# RUN go test -v schelly
