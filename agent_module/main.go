package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"regexp"
	"strings"
	"sync"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/redis/go-redis/v9"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.26.0"
	"go.opentelemetry.io/otel/trace"
)

type Agent struct {
	Role               string
	Rules              []string
	NATSSpecialization string
	AuctionSubjects    []string
	Specializations    map[string]float64 // Task type -> compatibility score (0.0 to 1.0)
	nc                 *nats.Conn
	mu                 sync.Mutex
	queueLength        int
	maxCapacity        int
	baseCost           float64
	costPerQueuedTask  float64
	tracer             trace.Tracer
	redisClient        *redis.Client
	stateKey           string
	counters           map[string]int64
	stats              map[string]float64
	cache              map[string]interface{}
}

type Message struct {
	Type    string                 `json:"type"`
	Data    map[string]interface{} `json:"data"`
	TraceID string                 `json:"trace_id"`
}

type Response struct {
	Result  interface{} `json:"result"`
	Error   string      `json:"error,omitempty"`
	TraceID string      `json:"trace_id"`
}

type AuctionRequest struct {
	TaskType string `json:"task_type"`
	TraceID  string `json:"trace_id"`
}

type Bid struct {
	AgentRole       string    `json:"agent_role"`
	Cost            float64   `json:"cost"`
	Compatibility   float64   `json:"compatibility"` // 0.0 to 1.0, higher is better
	EstimatedTime   int       `json:"estimated_time_ms"`
	CurrentLoad     int       `json:"current_load"`
	Capacity        int       `json:"capacity"`
	TraceID         string    `json:"trace_id"`
	Timestamp       time.Time `json:"timestamp"`
}

func parseMarkdownConfig(filename string) (*Agent, error) {
	content, err := os.ReadFile(filename)
	if err != nil {
		return nil, fmt.Errorf("failed to read config: %w", err)
	}

	agent := &Agent{
		Rules:              []string{},
		AuctionSubjects:    []string{},
		Specializations:    make(map[string]float64),
		maxCapacity:        100,
		baseCost:           1.0,
		costPerQueuedTask:  0.5,
	}

	lines := strings.Split(string(content), "\n")
	for i, line := range lines {
		line = strings.TrimSpace(line)

		if strings.HasPrefix(line, "# Role:") {
			agent.Role = strings.TrimSpace(strings.TrimPrefix(line, "# Role:"))
		}

		if strings.HasPrefix(line, "# NATS Specialization:") {
			agent.NATSSpecialization = strings.TrimSpace(strings.TrimPrefix(line, "# NATS Specialization:"))
		}

		if strings.HasPrefix(line, "# Auction Subjects:") {
			subjects := strings.TrimSpace(strings.TrimPrefix(line, "# Auction Subjects:"))
			for _, s := range strings.Split(subjects, ",") {
				trimmed := strings.TrimSpace(s)
				if trimmed != "" {
					agent.AuctionSubjects = append(agent.AuctionSubjects, trimmed)
				}
			}
		}

		if strings.HasPrefix(line, "# Specializations:") {
			specs := strings.TrimSpace(strings.TrimPrefix(line, "# Specializations:"))
			for _, spec := range strings.Split(specs, ",") {
				trimmed := strings.TrimSpace(spec)
				if trimmed != "" {
					parts := strings.Split(trimmed, "=")
					if len(parts) == 2 {
						taskType := strings.TrimSpace(parts[0])
						scoreStr := strings.TrimSpace(parts[1])
						var score float64
						if _, err := fmt.Sscanf(scoreStr, "%f", &score); err == nil {
							agent.Specializations[taskType] = score
						}
					}
				}
			}
		}

		if strings.HasPrefix(line, "## Rules") {
			for j := i + 1; j < len(lines); j++ {
				ruleLine := strings.TrimSpace(lines[j])
				if strings.HasPrefix(ruleLine, "#") {
					break
				}
				if ruleLine == "" {
					continue
				}
				if strings.HasPrefix(ruleLine, "- ") {
					rule := strings.TrimSpace(strings.TrimPrefix(ruleLine, "- "))
					agent.Rules = append(agent.Rules, rule)
				}
			}
			break
		}
	}

	if agent.Role == "" {
		return nil, fmt.Errorf("role not found in config")
	}
	if agent.NATSSpecialization == "" {
		return nil, fmt.Errorf("NATS specialization not found in config")
	}

	return agent, nil
}

func (a *Agent) ConnectRedis(redisAddr string) error {
	a.redisClient = redis.NewClient(&redis.Options{
		Addr:     redisAddr,
		Password: "",
		DB:       0,
	})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := a.redisClient.Ping(ctx).Err(); err != nil {
		return fmt.Errorf("failed to connect to Redis: %w", err)
	}

	a.stateKey = fmt.Sprintf("agent:%s:state", strings.ReplaceAll(a.Role, " ", "-"))
	a.counters = make(map[string]int64)
	a.stats = make(map[string]float64)
	a.cache = make(map[string]interface{})

	log.Printf("[%s] Connected to Redis at %s", a.Role, redisAddr)
	return nil
}

func (a *Agent) saveState(ctx context.Context) error {
	if a.redisClient == nil {
		return nil
	}

	a.mu.Lock()
	defer a.mu.Unlock()

	state := map[string]interface{}{
		"counters": a.counters,
		"stats":    a.stats,
		"cache":    a.cache,
	}

	stateJSON, err := json.Marshal(state)
	if err != nil {
		return fmt.Errorf("failed to marshal state: %w", err)
	}

	if err := a.redisClient.Set(ctx, a.stateKey, stateJSON, 0).Err(); err != nil {
		return fmt.Errorf("failed to save state to Redis: %w", err)
	}

	log.Printf("[%s] State saved to Redis (counters: %d, stats: %d, cache: %d)", a.Role, len(a.counters), len(a.stats), len(a.cache))
	return nil
}

func (a *Agent) restoreState(ctx context.Context) error {
	if a.redisClient == nil {
		return nil
	}

	stateJSON, err := a.redisClient.Get(ctx, a.stateKey).Result()
	if err == redis.Nil {
		log.Printf("[%s] No saved state found in Redis, starting fresh", a.Role)
		return nil
	}
	if err != nil {
		return fmt.Errorf("failed to get state from Redis: %w", err)
	}

	var state struct {
		Counters map[string]int64         `json:"counters"`
		Stats    map[string]float64       `json:"stats"`
		Cache    map[string]interface{}   `json:"cache"`
	}

	if err := json.Unmarshal([]byte(stateJSON), &state); err != nil {
		return fmt.Errorf("failed to unmarshal state: %w", err)
	}

	a.mu.Lock()
	a.counters = state.Counters
	a.stats = state.Stats
	a.cache = state.Cache
	a.mu.Unlock()

	log.Printf("[%s] State restored from Redis (counters: %d, stats: %d, cache: %d)", a.Role, len(a.counters), len(a.stats), len(a.cache))
	return nil
}

func (a *Agent) incrementCounter(name string, delta int64) int64 {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.counters[name] += delta
	return a.counters[name]
}

func (a *Agent) updateStat(name string, value float64) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.stats[name] = value
}

func (a *Agent) getCache(key string) (interface{}, bool) {
	a.mu.Lock()
	defer a.mu.Unlock()
	val, ok := a.cache[key]
	return val, ok
}

func (a *Agent) setCache(key string, value interface{}) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.cache[key] = value
}

func initTracer(serviceName string) (trace.Tracer, error) {
	ctx := context.Background()

	exporter, err := otlptracegrpc.New(ctx,
		otlptracegrpc.WithInsecure(),
		otlptracegrpc.WithEndpoint("localhost:4317"),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create OTLP exporter: %w", err)
	}

	res, err := resource.New(ctx,
		resource.WithAttributes(
			semconv.ServiceName(serviceName),
			semconv.ServiceVersion("1.0.0"),
		),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create resource: %w", err)
	}

	tp := sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(exporter),
		sdktrace.WithResource(res),
	)

	otel.SetTracerProvider(tp)

	return tp.Tracer("agent-tracer"), nil
}

func (a *Agent) Connect(natsURL string) error {
	var err error
	a.nc, err = nats.Connect(natsURL)
	if err != nil {
		return fmt.Errorf("failed to connect to NATS: %w", err)
	}
	return nil
}

func (a *Agent) Start() error {
	if a.nc == nil {
		return fmt.Errorf("not connected to NATS")
	}

	queue := a.NATSSpecialization
	log.Printf("[%s] Agent starting, listening on queue: %s", a.Role, queue)

	groupName := strings.ToLower(strings.ReplaceAll(a.Role, " ", "-"))
	_, err := a.nc.QueueSubscribe(queue, groupName, func(msg *nats.Msg) {
		a.handleMessage(msg)
	})
	if err != nil {
		return fmt.Errorf("failed to subscribe: %w", err)
	}

	for _, auctionSubject := range a.AuctionSubjects {
		log.Printf("[%s] Subscribing to auction subject: %s", a.Role, auctionSubject)
		_, err := a.nc.Subscribe(auctionSubject, func(msg *nats.Msg) {
			a.handleAuction(msg)
		})
		if err != nil {
			return fmt.Errorf("failed to subscribe to auction %s: %w", auctionSubject, err)
		}
	}

	log.Printf("[%s] Subscribed successfully. Waiting for messages...", a.Role)
	return nil
}

func (a *Agent) handleMessage(msg *nats.Msg) {
	ctx := context.Background()
	var request Message
	if err := json.Unmarshal(msg.Data, &request); err != nil {
		log.Printf("[%s] Failed to unmarshal message: %v", a.Role, err)
		return
	}

	ctx, span := a.tracer.Start(ctx, "handleMessage",
		trace.WithAttributes(
			attribute.String("agent.role", a.Role),
			attribute.String("message.type", request.Type),
			attribute.String("trace.id", request.TraceID),
		),
	)
	defer span.End()

	log.Printf("[%s] Processing %s (trace: %s)", a.Role, request.Type, request.TraceID)

	a.mu.Lock()
	a.queueLength++
	span.SetAttributes(attribute.Int("agent.queue_length", a.queueLength))
	a.mu.Unlock()

	result, respErr := a.process(ctx, request)

	response := Response{
		Result:  result,
		TraceID: request.TraceID,
	}
	if respErr != nil {
		response.Error = respErr.Error()
		span.SetStatus(codes.Error, respErr.Error())
		span.RecordError(respErr)
	}

	respData, _ := json.Marshal(response)
	if err := a.nc.Publish(msg.Reply, respData); err != nil {
		log.Printf("[%s] Failed to publish reply: %v", a.Role, err)
		span.SetStatus(codes.Error, "Failed to publish reply")
		span.RecordError(err)
	}

	a.mu.Lock()
	a.queueLength--
	a.mu.Unlock()
}

func (a *Agent) handleAuction(msg *nats.Msg) {
	ctx := context.Background()
	var request AuctionRequest
	if err := json.Unmarshal(msg.Data, &request); err != nil {
		log.Printf("[%s] Failed to unmarshal auction: %v", a.Role, err)
		return
	}

	ctx, span := a.tracer.Start(ctx, "handleAuction",
		trace.WithAttributes(
			attribute.String("agent.role", a.Role),
			attribute.String("auction.task_type", request.TaskType),
			attribute.String("trace.id", request.TraceID),
		),
	)
	defer span.End()

	log.Printf("[%s] Received auction for %s (trace: %s)", a.Role, request.TaskType, request.TraceID)

	bid := a.calculateBid(ctx, request)
	bidData, _ := json.Marshal(bid)

	if err := a.nc.Publish(msg.Reply, bidData); err != nil {
		log.Printf("[%s] Failed to publish bid: %v", a.Role, err)
		span.SetStatus(codes.Error, "Failed to publish bid")
		span.RecordError(err)
	}

	span.SetAttributes(
		attribute.Float64("bid.cost", bid.Cost),
		attribute.Int("bid.current_load", bid.CurrentLoad),
		attribute.Int("bid.capacity", bid.Capacity),
		attribute.Int("bid.estimated_time_ms", bid.EstimatedTime),
	)
	log.Printf("[%s] Submitted bid: cost=%.2f, load=%d/%d (trace: %s)", a.Role, bid.Cost, bid.CurrentLoad, bid.Capacity, request.TraceID)
}

func (a *Agent) process(ctx context.Context, msg Message) (interface{}, error) {
	ctx, span := a.tracer.Start(ctx, "process",
		trace.WithAttributes(
			attribute.String("message.type", msg.Type),
		),
	)
	defer span.End()

	for _, rule := range a.Rules {
		if matchesRule(rule, msg.Type) {
			return a.applyRule(ctx, rule, msg.Data)
		}
	}
	err := fmt.Errorf("no matching rule for type: %s", msg.Type)
	span.SetStatus(codes.Error, err.Error())
	span.RecordError(err)
	return nil, err
}

func matchesRule(rule, msgType string) bool {
	pattern := strings.Split(rule, ":")[0]
	matched, _ := regexp.MatchString(pattern, msgType)
	return matched
}

func (a *Agent) applyRule(ctx context.Context, rule string, data map[string]interface{}) (interface{}, error) {
	ctx, span := a.tracer.Start(ctx, "applyRule",
		trace.WithAttributes(
			attribute.String("agent.role", a.Role),
			attribute.String("rule", rule),
		),
	)
	defer span.End()

	parts := strings.SplitN(rule, ":", 2)
	if len(parts) < 2 {
		err := fmt.Errorf("invalid rule format")
		span.SetStatus(codes.Error, err.Error())
		span.RecordError(err)
		return nil, err
	}

	action := strings.TrimSpace(parts[1])
	log.Printf("[%s] Applying action: %s", a.Role, action)

	return map[string]interface{}{
		"action": action,
		"status": "processed",
		"input":  data,
	}, nil
}

func (a *Agent) calculateBid(ctx context.Context, request AuctionRequest) Bid {
	ctx, span := a.tracer.Start(ctx, "calculateBid")
	defer span.End()

	a.mu.Lock()
	defer a.mu.Unlock()

	loadRatio := float64(a.queueLength) / float64(a.maxCapacity)
	cost := a.baseCost + (a.costPerQueuedTask * float64(a.queueLength))
	estimatedTime := 100 + (int(loadRatio) * 1000)

	// Calculate compatibility score based on specializations
	compatibility := 0.5 // Default compatibility
	if a.Specializations != nil {
		if score, ok := a.Specializations[request.TaskType]; ok {
			compatibility = score
		}
	}

	span.SetAttributes(
		attribute.Float64("bid.load_ratio", loadRatio),
		attribute.Float64("bid.base_cost", a.baseCost),
		attribute.Float64("bid.cost_per_queued_task", a.costPerQueuedTask),
		attribute.Float64("bid.compatibility", compatibility),
	)

	return Bid{
		AgentRole:     a.Role,
		Cost:          cost,
		Compatibility: compatibility,
		EstimatedTime: estimatedTime,
		CurrentLoad:   a.queueLength,
		Capacity:      a.maxCapacity,
		TraceID:       request.TraceID,
		Timestamp:     time.Now(),
	}
}

func (a *Agent) Close() {
	ctx := context.Background()
	if err := a.saveState(ctx); err != nil {
		log.Printf("[%s] Failed to save state on shutdown: %v", a.Role, err)
	}
	if a.nc != nil {
		a.nc.Close()
	}
	if a.redisClient != nil {
		a.redisClient.Close()
	}
}

func main() {
	configFile := flag.String("config", "configs/income-analyzer-config.md", "Path to markdown config file")
	natsURL := flag.String("nats", nats.DefaultURL, "NATS server URL")
	redisURL := flag.String("redis", "localhost:6379", "Redis server URL")
	flag.Parse()

	agent, err := parseMarkdownConfig(*configFile)
	if err != nil {
		log.Fatalf("Failed to parse config: %v", err)
	}

	log.Printf("Loaded agent: Role=%s, Specialization=%s, Rules=%d, AuctionSubjects=%d", agent.Role, agent.NATSSpecialization, len(agent.Rules), len(agent.AuctionSubjects))

	// Initialize OpenTelemetry tracer
	tracer, err := initTracer(agent.Role)
	if err != nil {
		log.Printf("Failed to initialize tracer (continuing without tracing): %v", err)
	} else {
		agent.tracer = tracer
		log.Printf("OpenTelemetry tracer initialized for: %s", agent.Role)
	}

	// Connect to Redis for state persistence
	if err := agent.ConnectRedis(*redisURL); err != nil {
		log.Printf("Failed to connect to Redis (continuing without state persistence): %v", err)
	} else {
		// Restore state from Redis
		if err := agent.restoreState(context.Background()); err != nil {
			log.Printf("Failed to restore state from Redis: %v", err)
		}
	}

	if err := agent.Connect(*natsURL); err != nil {
		log.Fatalf("Failed to connect: %v", err)
	}
	defer agent.Close()

	if err := agent.Start(); err != nil {
		log.Fatalf("Failed to start: %v", err)
	}

	select {}
}
