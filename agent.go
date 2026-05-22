package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"regexp"
	"strings"

	"github.com/nats-io/nats.go"
)

type Agent struct {
	Role               string
	Rules              []string
	NATSSpecialization string
	nc                 *nats.Conn
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

func parseMarkdownConfig(filename string) (*Agent, error) {
	content, err := os.ReadFile(filename)
	if err != nil {
		return nil, fmt.Errorf("failed to read config: %w", err)
	}

	agent := &Agent{
		Rules: []string{},
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

	_, err := a.nc.QueueSubscribe(queue, a.Role, func(msg *nats.Msg) {
		a.handleMessage(msg)
	})
	if err != nil {
		return fmt.Errorf("failed to subscribe: %w", err)
	}

	log.Printf("[%s] Subscribed successfully. Waiting for messages...", a.Role)
	return nil
}

func (a *Agent) handleMessage(msg *nats.Msg) {
	var request Message
	if err := json.Unmarshal(msg.Data, &request); err != nil {
		log.Printf("[%s] Failed to unmarshal message: %v", a.Role, err)
		return
	}

	log.Printf("[%s] Processing %s (trace: %s)", a.Role, request.Type, request.TraceID)

	result, respErr := a.process(request)

	response := Response{
		Result:  result,
		TraceID: request.TraceID,
	}
	if respErr != nil {
		response.Error = respErr.Error()
	}

	respData, _ := json.Marshal(response)
	if err := a.nc.Publish(msg.Reply, respData); err != nil {
		log.Printf("[%s] Failed to publish reply: %v", a.Role, err)
	}
}

func (a *Agent) process(msg Message) (interface{}, error) {
	for _, rule := range a.Rules {
		if matchesRule(rule, msg.Type) {
			return a.applyRule(rule, msg.Data)
		}
	}
	return nil, fmt.Errorf("no matching rule for type: %s", msg.Type)
}

func matchesRule(rule, msgType string) bool {
	pattern := strings.Split(rule, ":")[0]
	matched, _ := regexp.MatchString(pattern, msgType)
	return matched
}

func (a *Agent) applyRule(rule string, data map[string]interface{}) (interface{}, error) {
	parts := strings.SplitN(rule, ":", 2)
	if len(parts) < 2 {
		return nil, fmt.Errorf("invalid rule format")
	}

	action := strings.TrimSpace(parts[1])
	log.Printf("[%s] Applying action: %s", a.Role, action)

	return map[string]interface{}{
		"action": action,
		"status": "processed",
		"input":  data,
	}, nil
}

func (a *Agent) Close() {
	if a.nc != nil {
		a.nc.Close()
	}
}

func main() {
	configFile := flag.String("config", "agent-config.md", "Path to markdown config file")
	natsURL := flag.String("nats", nats.DefaultURL, "NATS server URL")
	flag.Parse()

	agent, err := parseMarkdownConfig(*configFile)
	if err != nil {
		log.Fatalf("Failed to parse config: %v", err)
	}

	log.Printf("Loaded agent: Role=%s, Specialization=%s, Rules=%d", agent.Role, agent.NATSSpecialization, len(agent.Rules))

	if err := agent.Connect(*natsURL); err != nil {
		log.Fatalf("Failed to connect: %v", err)
	}
	defer agent.Close()

	if err := agent.Start(); err != nil {
		log.Fatalf("Failed to start: %v", err)
	}

	select {}
}
