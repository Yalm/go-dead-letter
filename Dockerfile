FROM scratch
COPY ./build /go/bin/app
ENTRYPOINT ["/go/bin/app"]