FROM golang:alpine AS build
WORKDIR /app
COPY . .
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -ldflags="-w -s" -o /go/bin/app

FROM scratch
COPY --from=build /go/bin/app /go/bin/app
ENTRYPOINT ["/go/bin/app"]