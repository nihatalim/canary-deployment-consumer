FROM golang:1.20.5-alpine3.18 AS builder

WORKDIR /app

COPY go.mod ./
COPY *.go ./
COPY config ./config
COPY resources ./resources

RUN go mod download
RUN go mod tidy

RUN go build -o consumer

FROM scratch

COPY --from=builder /app/consumer /consumer

CMD [ "/consumer" ]