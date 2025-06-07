FROM golang:1.24-alpine AS builder

WORKDIR /app

COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -a -ldflags="-w -s" -o plagiarism-detector ./src
FROM alpine:latest
WORKDIR /root/
COPY --from=builder /app/plagiarism-detector .
COPY .env .

CMD ["./plagiarism-detector"]
