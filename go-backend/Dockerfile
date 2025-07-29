FROM golang:1.22.4-alpine3.20 AS golang

WORKDIR /app
COPY . .

RUN go mod download
RUN go mod verify

RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -o /ticker .

FROM gcr.io/distroless/static-debian11

COPY --from=golang /ticker .

EXPOSE 8080

CMD ["/ticker"]