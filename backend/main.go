package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"log"
	"sync"
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

	wsClients   = make(map[*websocket.Conn]bool)
	wsClientsMu sync.Mutex
)

type Task struct {
	ID         int    `json:"id"`
	Title      string `json:"title"`
	AssignedTo string `json:"assigned_to"`
	Status     string `json:"status"`
	UserID     string `json:"user_id"`
}

func main() {

	connStr := "host=postgres user=kanban_user password=ultra_secret dbname=kanban_db sslmode=disable"
	var err error
	db, err = sql.Open("postgres", connStr)
	if err != nil {
		log.Fatal("Error abriendo DB:", err)
	}
	if err = db.Ping(); err != nil {
		log.Fatal("DB no responde:", err)
	}
	log.Println("Conectado a PostgreSQL")

	rdb = redis.NewClient(&redis.Options{Addr: "redis:6379"})
	if _, err = rdb.Ping(context.Background()).Result(); err != nil {
		log.Fatal("Redis no responde:", err)
	}
	log.Println("Conectado a Redis")

	nc, err = nats.Connect("nats:4222")
	if err != nil {
		log.Fatal("NATS no responde:", err)
	}

	js, err := nc.JetStream()
	if err == nil {
		js.AddStream(&nats.StreamConfig{
			Name:     "TASK_STREAM",
			Subjects: []string{"task.created", "task.updated"},
			MaxAge:   24 * time.Hour,
		})
	}
	log.Println("Conectado a NATS")

	go listenToNATS()

	app := fiber.New()

	app.Use(cors.New(cors.Config{
		AllowOrigins:     "https://tudominio.com, http://localhost:5173, http://localhost:3000",
		AllowMethods:     "GET,POST,PUT,DELETE,OPTIONS",
		AllowHeaders:     "Origin, Content-Type, Accept, Authorization, X-User-ID, X-User-Role",
		AllowCredentials: true,
	}))

	app.Use(rateLimiter)

	app.Get("/api/tasks", getTasks)
	app.Post("/api/tasks", createTask)
	app.Get("/ws", websocket.New(handleWebSocket))

	log.Println("Servidor Go escuchando en :3000")
	log.Fatal(app.Listen(":3000"))
}

func rateLimiter(c *fiber.Ctx) error {
	ip := c.Get("X-Forwarded-For")
	if ip == "" {
		ip = c.IP()
	}
	key := "rate_limit:" + ip
	ctx := context.Background()

	count, err := rdb.Incr(ctx, key).Result()
	if err != nil {
		return c.Status(500).SendString("Error interno de Rate Limit")
	}
	if count == 1 {
		rdb.Expire(ctx, key, 60*time.Second)
	}
	if count > 100 {
		return c.Status(429).SendString("Demasiadas peticiones (100/minuto). Espera 60s.")
	}
	return c.Next()
}

func getTasks(c *fiber.Ctx) error {
	ctx := context.Background()
	cacheKey := "tasks:all"

	cached, err := rdb.Get(ctx, cacheKey).Result()
	if err == nil {
		return c.Type("json").SendString(cached)
	}

	rows, err := db.Query("SELECT id, title, assigned_to, status, user_id FROM tasks")
	if err != nil {
		return c.Status(500).SendString("Error en DB")
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

	return c.Type("json").Send(jsonData)
}

func createTask(c *fiber.Ctx) error {
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
		log.Println("Error insertando:", err)
		return c.Status(500).SendString("Error al guardar en DB")
	}
	newTask.ID = id

	rdb.Del(context.Background(), "tasks:all")

	eventData, _ := json.Marshal(newTask)
	nc.Publish("task.created", eventData)

	return c.Status(201).JSON(newTask)
}

func handleWebSocket(c *websocket.Conn) {

	wsClientsMu.Lock()
	wsClients[c] = true
	wsClientsMu.Unlock()
	log.Printf("Cliente conectado. Total conectados: %d", len(wsClients))

	defer func() {
		wsClientsMu.Lock()
		delete(wsClients, c)
		wsClientsMu.Unlock()
		c.Close()
		log.Printf("Cliente desconectado. Total conectados: %d", len(wsClients))
	}()

	for {
		_, msg, err := c.ReadMessage()
		if err != nil {
			break
		}

		nc.Publish("task.updated", msg)
	}
}

func listenToNATS() {

	_, err := nc.Subscribe("task.created", func(msg *nats.Msg) {

		wsClientsMu.Lock()
		defer wsClientsMu.Unlock()

		for client := range wsClients {
			err := client.WriteMessage(websocket.TextMessage, msg.Data)
			if err != nil {

				delete(wsClients, client)
				client.Close()
			}
		}
	})

	if err != nil {
		log.Println("Error suscribiendo a NATS:", err)
		return
	}

	select {}
}
