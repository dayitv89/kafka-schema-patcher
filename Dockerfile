FROM golang:1.16.3-alpine3.13 AS COMPILE_MACHINE
RUN apk --no-cache add git build-base
WORKDIR /go/src/github.com/dayitv89/kafka-schema-patcher
COPY . .
RUN go mod vendor
RUN go build -o main main.go

FROM alpine:3.13
RUN apk update \
  && apk --no-cache add ca-certificates nano bind-tools \
  && rm -rf /var/cache/apk/*
WORKDIR /root
COPY --from=COMPILE_MACHINE /go/src/github.com/dayitv89/kafka-schema-patcher/main .
COPY --from=COMPILE_MACHINE /go/src/github.com/dayitv89/kafka-schema-patcher/.env.default .env
ENTRYPOINT ./main
