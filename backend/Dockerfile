FROM golang:1.21-alpine AS builder

WORKDIR /app

COPY go.mod go.sum ./
RUN go mod download

COPY . .

RUN go build -o /kanban-server main.golang

FROM alpine:latest

WORKDIR /root/

COPY --from=builder /kanban-server .

EXPOSE 3000

CMD ["./kanban-server"]