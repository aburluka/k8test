FROM golang:1.23 AS builder

RUN mkdir /build
COPY . /build
WORKDIR /build

RUN CGO_ENABLED=0 GOOS=linux go build -a -o app ./cmd/bucketserver

FROM alpine:3.20
RUN apk update && apk upgrade -U -a
COPY --from=builder /build/app .
EXPOSE 6571
ENTRYPOINT ["./app"]
