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
	nc                 *nats.Conn
	mu                 sync.Mutex
	queueLength        int
	maxCapacity        int
	baseCost           float64
	costPerQueuedTask  float64
	tracer             trace.Tracer
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

	span.SetAttributes(
		attribute.Float64("bid.load_ratio", loadRatio),
		attribute.Float64("bid.base_cost", a.baseCost),
		attribute.Float64("bid.cost_per_queued_task", a.costPerQueuedTask),
	)

	return Bid{
		AgentRole:     a.Role,
		Cost:          cost,
		EstimatedTime: estimatedTime,
		CurrentLoad:   a.queueLength,
		Capacity:      a.maxCapacity,
		TraceID:       request.TraceID,
		Timestamp:     time.Now(),
	}
}

func (a *Agent) Close() {
	if a.nc != nil {
		a.nc.Close()
	}
}

func main() {
	configFile := flag.String("config", "configs/income-analyzer-config.md", "Path to markdown config file")
	natsURL := flag.String("nats", nats.DefaultURL, "NATS server URL")
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

	if err := agent.Connect(*natsURL); err != nil {
		log.Fatalf("Failed to connect: %v", err)
	}
	defer agent.Close()

	if err := agent.Start(); err != nil {
		log.Fatalf("Failed to start: %v", err)
	}

	select {}
}
