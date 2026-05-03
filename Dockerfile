FROM golang:1.26-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -o /out/scipio ./cmd/scipio

FROM alpine:3.22
WORKDIR /app
COPY --from=build /out/scipio /usr/local/bin/scipio
COPY migrations /app/migrations
EXPOSE 8080
EXPOSE 9090
ENTRYPOINT ["scipio"]
