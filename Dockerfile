FROM golang:1.10 AS BUILD

#doing dependency build separated from source build optimizes time for developer, but is not required
#install external dependencies first
ADD /main.go $GOPATH/src/schelly/main.go
RUN go get github.com/stretchr/testify
RUN go get -v schelly

#now build source code
ADD schelly $GOPATH/src/schelly
RUN go get -v schelly


FROM golang:1.10 AS IMAGE

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

ENV RETENTION_SECONDLY    0@L
ENV RETENTION_MINUTELY    0@L
ENV RETENTION_HOURLY      0@L
ENV RETENTION_DAILY       4@L
ENV RETENTION_WEEKLY      4@L
ENV RETENTION_MONTHLY     3@L
ENV RETENTION_YEARLY      2@L

COPY --from=BUILD /go/bin/* /bin/
ADD startup.sh /

CMD [ "/startup.sh" ]


FROM BUILD AS TEST
RUN go test -v schelly
