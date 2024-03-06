FROM alpine:latest
WORKDIR /app

COPY ./consumer .

ENTRYPOINT [ "./consumer" ]