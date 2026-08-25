package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"time"

	"github.com/gofiber/fiber/v2"
	"github.com/gofiber/fiber/v2/middleware/cors"

	"github.com/gofiber/websocket/v2"
	_ "github.com/lib/pq"
	"github.com/nats-io/nats.go"
	"github.com/redis/go-redis/v9"
)

var (
	db  *sql.DB
	rdb *redis.Client
	nc  *nats.Conn
	js  nats.JetStreamContext
)

type Task struct {
	ID         int    `json:"id"`
	Title      string `json:"title"`
	AssignedTo string `json:"assigned_to"`
	Status     string `json:"status"`
	UserID     string `json:"user_id"`
}

func main() {

	connStr := fmt.Sprintf("host=%s user=%s password=%s dbname=%s sslmode=disable",
		"postgres", "kanban_user", "ultra_secret", "kanban_db")
	var err error
	db, err = sql.Open("postgres", connStr)
	if err != nil {
		log.Fatal("Error en DB:", err)
	}

	if err = db.Ping(); err != nil {
		log.Fatal("DB no responde:", err)
	}
	log.Println("Conectado a PostgreSQL")

	rdb = redis.NewClient(&redis.Options{
		Addr: "redis:6379",
	})
	if _, err = rdb.Ping(context.Background()).Result(); err != nil {
		log.Fatal("Redis no responde:", err)
	}
	log.Println("Conectado a Redis")

	nc, err = nats.Connect("nats:4222")
	if err != nil {
		log.Fatal("NATS no responde:", err)
	}

	js, err = nc.JetStream()
	if err != nil {
		log.Fatal("JetStream no disponible:", err)
	}

	js.AddStream(&nats.StreamConfig{
		Name:     "TASK_STREAM",
		Subjects: []string{"task.created", "task.updated"},
		MaxAge:   24 * time.Hour,
	})
	log.Println("Conectado a NATS")

	app := fiber.New()

	app.Use(cors.New(cors.Config{
		AllowOrigins:     "https://tudominio.com, http://localhost:5173",
		AllowMethods:     "GET,POST,PUT,DELETE,OPTIONS",
		AllowHeaders:     "Origin, Content-Type, Accept, Authorization",
		AllowCredentials: true,
	}))

	app.Use(func(c *fiber.Ctx) error {

		ip := c.Get("X-Forwarded-For")
		if ip == "" {
			ip = c.IP()
		}
		key := "rate_limit:" + ip

		ctx := context.Background()

		count, err := rdb.Incr(ctx, key).Result()
		if err != nil {
			return c.Status(500).SendString("Error interno")
		}
		if count == 1 {

			rdb.Expire(ctx, key, 60*time.Second)
		}
		if count > 100 {

			return c.Status(429).SendString("Demasiadas peticiones, espera 60 segundos")
		}
		return c.Next()
	})

	app.Get("/api/tasks", func(c *fiber.Ctx) error {

		cacheKey := "tasks:all"
		ctx := context.Background()
		cached, err := rdb.Get(ctx, cacheKey).Result()
		if err == nil {

			c.Set("Content-Type", "application/json")
			return c.SendString(cached)
		}

		rows, err := db.Query("SELECT id, title, assigned_to, status, user_id FROM tasks")
		if err != nil {
			return c.Status(500).SendString("DB Error")
		}
		defer rows.Close()

		var tasks []Task
		for rows.Next() {
			var t Task

			rows.Scan(&t.ID, &t.Title, &t.AssignedTo, &t.Status, &t.UserID)
			tasks = append(tasks, t)
		}

		jsonData, _ := json.Marshal(tasks)

		rdb.SetEx(ctx, cacheKey, jsonData, 30*time.Second)

		return c.Send(jsonData)
	})

	app.Post("/api/tasks", func(c *fiber.Ctx) error {
		var newTask Task

		if err := c.BodyParser(&newTask); err != nil {
			return c.Status(400).SendString("JSON inválido")
		}

		var id int
		err := db.QueryRow(
			"INSERT INTO tasks (title, assigned_to, status, user_id) VALUES ($1, $2, $3, $4) RETURNING id",
			newTask.Title, newTask.AssignedTo, newTask.Status, newTask.UserID,
		).Scan(&id)
		if err != nil {
			return c.Status(500).SendString("Error al insertar")
		}
		newTask.ID = id

		rdb.Del(context.Background(), "tasks:all")

		eventData, _ := json.Marshal(newTask)

		nc.Publish("task.created", eventData)

		return c.Status(201).JSON(newTask)
	})

	app.Use("/ws", func(c *fiber.Ctx) error {

		if c.Get("Upgrade") != "websocket" {
			return c.Status(400).SendString("Se requiere WebSocket")
		}
		return c.Next()
	})

	app.Get("/ws", websocket.New(func(c *websocket.Conn) {

		var clients []*websocket.Conn
		clients = append(clients, c)

		sub, err := nc.Subscribe("task.created", func(msg *nats.Msg) {

			for _, client := range clients {

				client.WriteMessage(websocket.TextMessage, msg.Data)
			}
		})
		if err != nil {
			log.Println("Error suscribiendo a NATS:", err)
			return
		}

		defer sub.Unsubscribe()

		for {
			_, msg, err := c.ReadMessage()
			if err != nil {

				break
			}

			nc.Publish("task.updated", msg)
		}
	}))

	log.Fatal(app.Listen(":3000"))
}
