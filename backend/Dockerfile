FROM golang:1.25-alpine AS builder

WORKDIR /app

ENV GOPROXY=https://goproxy.io,direct

COPY go.mod go.sum* ./
RUN go mod download

COPY . .

RUN go build -o /kanban-server main.go

FROM alpine:latest

WORKDIR /root/

COPY --from=builder /kanban-server .

EXPOSE 3000

CMD ["./kanban-server"]