FROM golang:1.23.4-alpine AS build
WORKDIR /src
RUN apk add --no-cache ca-certificates git
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/wager-service ./cmd/wager-service
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/migrate ./cmd/migrate

FROM alpine:3.21
RUN apk add --no-cache ca-certificates
WORKDIR /app
COPY --from=build /out/wager-service /usr/local/bin/wager-service
COPY --from=build /out/migrate /usr/local/bin/migrate
EXPOSE 8080
ENTRYPOINT ["/bin/sh", "-c", "migrate up && exec wager-service"]