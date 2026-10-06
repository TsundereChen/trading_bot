FROM docker.io/library/golang:1.26-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /trader .

FROM docker.io/library/alpine:3.23
RUN apk add --no-cache ca-certificates && addgroup -g 10001 trader && adduser -D -u 10001 -G trader trader
WORKDIR /app
RUN mkdir -p /app/data && chown trader:trader /app/data
COPY --from=build /trader /usr/local/bin/trader
USER trader
EXPOSE 8080 9091
ENTRYPOINT ["/usr/local/bin/trader"]
