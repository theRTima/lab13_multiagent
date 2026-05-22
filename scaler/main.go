package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"os/exec"
	"strings"
	"sync"
	"time"

	"github.com/nats-io/nats.go"
)

type Scaler struct {
	nc          *nats.Conn
	scaleThreshold int
	scaleDownThreshold int
	minInstances int
	maxInstances int
	checkInterval time.Duration
	agentImage string
	agentConfigs map[string]string
	runningContainers map[string]string
	mu sync.Mutex
}

type AgentStatus struct {
	Role        string
	QueueLength int
	ContainerID string
}

func main() {
	dockerURL := flag.String("docker", "unix:///var/run/docker.sock", "Docker daemon URL")
	natsURL := flag.String("nats", nats.DefaultURL, "NATS server URL")
	scaleThreshold := flag.Int("scale-threshold", 5, "Queue length threshold to scale up")
	scaleDownThreshold := flag.Int("scale-down-threshold", 2, "Queue length threshold to scale down")
	minInstances := flag.Int("min-instances", 1, "Minimum number of agent instances")
	maxInstances := flag.Int("max-instances", 5, "Maximum number of agent instances")
	checkInterval := flag.Duration("check-interval", 10*time.Second, "Interval between scaling checks")
	agentImage := flag.String("agent-image", "agent:latest", "Docker image for agent containers")
	flag.Parse()

	scaler, err := NewScaler(*dockerURL, *natsURL, *scaleThreshold, *scaleDownThreshold, *minInstances, *maxInstances, *checkInterval, *agentImage)
	if err != nil {
		log.Fatalf("Failed to create scaler: %v", err)
	}
	defer scaler.Close()

	// Register agent configs
	scaler.RegisterAgent("income-analyzer", "configs/income-analyzer-config.md")
	scaler.RegisterAgent("data-collection", "configs/data-collection-config.md")
	scaler.RegisterAgent("risk-evaluation", "configs/risk-evaluation-config.md")

	log.Printf("Scaler started with threshold=%d, scale-down=%d, min=%d, max=%d", 
		*scaleThreshold, *scaleDownThreshold, *minInstances, *maxInstances)

	if err := scaler.Run(context.Background()); err != nil {
		log.Fatalf("Scaler failed: %v", err)
	}
}

func NewScaler(dockerURL, natsURL string, scaleThreshold, scaleDownThreshold, minInstances, maxInstances int, checkInterval time.Duration, agentImage string) (*Scaler, error) {
	nc, err := nats.Connect(natsURL)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to NATS: %w", err)
	}

	return &Scaler{
		nc: nc,
		scaleThreshold: scaleThreshold,
		scaleDownThreshold: scaleDownThreshold,
		minInstances: minInstances,
		maxInstances: maxInstances,
		checkInterval: checkInterval,
		agentImage: agentImage,
		agentConfigs: make(map[string]string),
		runningContainers: make(map[string]string),
	}, nil
}

func (s *Scaler) RegisterAgent(role, configPath string) {
	s.agentConfigs[role] = configPath
	log.Printf("Registered agent: %s with config: %s", role, configPath)
}

func (s *Scaler) Run(ctx context.Context) error {
	ticker := time.NewTicker(s.checkInterval)
	defer ticker.Stop()

	// Initial scale to minimum instances
	for role := range s.agentConfigs {
		if err := s.ensureMinInstances(ctx, role); err != nil {
			log.Printf("Failed to ensure min instances for %s: %v", role, err)
		}
	}

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			s.checkAndScale(ctx)
		}
	}
}

func (s *Scaler) checkAndScale(ctx context.Context) {
	for role := range s.agentConfigs {
		status, err := s.getAgentStatus(ctx, role)
		if err != nil {
			log.Printf("Failed to get status for %s: %v", role, err)
			continue
		}

		currentCount := s.getRunningContainerCount(role)
		log.Printf("[%s] Queue: %d, Running: %d/%d", role, status.QueueLength, currentCount, s.maxInstances)

		if status.QueueLength > s.scaleThreshold && currentCount < s.maxInstances {
			log.Printf("[%s] Scaling UP (queue=%d > threshold=%d)", role, status.QueueLength, s.scaleThreshold)
			if err := s.scaleUp(ctx, role); err != nil {
				log.Printf("Failed to scale up %s: %v", role, err)
			}
		} else if status.QueueLength < s.scaleDownThreshold && currentCount > s.minInstances {
			log.Printf("[%s] Scaling DOWN (queue=%d < threshold=%d)", role, status.QueueLength, s.scaleDownThreshold)
			if err := s.scaleDown(ctx, role); err != nil {
				log.Printf("Failed to scale down %s: %v", role, err)
			}
		}
	}
}

func (s *Scaler) getAgentStatus(ctx context.Context, role string) (*AgentStatus, error) {
	// Query actual agent status via NATS
	subject := fmt.Sprintf("agent.status.%s", strings.ReplaceAll(role, " ", "."))
	
	// Send request for status
	request := map[string]string{
		"type": "status_request",
		"role": role,
	}
	requestData, err := json.Marshal(request)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal request: %w", err)
	}

	// Subscribe to response
	responseSubject := nats.NewInbox()
	sub, err := s.nc.SubscribeSync(responseSubject)
	if err != nil {
		return nil, fmt.Errorf("failed to subscribe to response: %w", err)
	}
	defer sub.Unsubscribe()

	// Publish request
	if err := s.nc.PublishRequest(subject, responseSubject, requestData); err != nil {
		return nil, fmt.Errorf("failed to publish request: %w", err)
	}

	// Wait for response with timeout
	msg, err := sub.NextMsgWithContext(ctx)
	if err != nil {
		// If no response, use Docker to count containers as fallback
		return s.getAgentStatusFallback(ctx, role)
	}

	// Parse response
	var status struct {
		Role        string `json:"role"`
		QueueLength int    `json:"queue_length"`
		Status      string `json:"status"`
	}
	if err := json.Unmarshal(msg.Data, &status); err != nil {
		return nil, fmt.Errorf("failed to unmarshal response: %w", err)
	}

	return &AgentStatus{
		Role:        role,
		QueueLength: status.QueueLength,
	}, nil
}

func (s *Scaler) getAgentStatusFallback(ctx context.Context, role string) (*AgentStatus, error) {
	// Fallback: Use Docker to count containers and estimate queue
	cmd := exec.CommandContext(ctx, "docker", "ps", "-q", 
		"--filter", fmt.Sprintf("label=agent.role=%s", role))
	output, err := cmd.CombinedOutput()
	if err != nil {
		return nil, fmt.Errorf("failed to list containers: %w, output: %s", err, string(output))
	}

	containerCount := len(strings.Fields(string(output)))
	
	// If no containers, assume no queue
	if containerCount == 0 {
		return &AgentStatus{
			Role:        role,
			QueueLength: 0,
		}, nil
	}

	// If containers exist but no NATS response, assume moderate load
	return &AgentStatus{
		Role:        role,
		QueueLength: containerCount, // Conservative estimate
	}, nil
}

func (s *Scaler) getRunningContainerCount(role string) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	count := 0
	for _, r := range s.runningContainers {
		if r == role {
			count++
		}
	}
	return count
}

func (s *Scaler) scaleUp(ctx context.Context, role string) error {
	configPath, ok := s.agentConfigs[role]
	if !ok {
		return fmt.Errorf("no config found for role: %s", role)
	}

	containerName := fmt.Sprintf("agent-%s-%d", strings.ReplaceAll(role, " ", "-"), time.Now().Unix())
	
	// Use Docker CLI to run container
	cmd := exec.CommandContext(ctx, "docker", "run", "-d",
		"--name", containerName,
		"--label", fmt.Sprintf("agent.role=%s", role),
		"--label", "managed-by=scaler",
		"--network", "host",
		"--restart", "unless-stopped",
		"-e", fmt.Sprintf("CONFIG=%s", configPath),
		"-e", fmt.Sprintf("NATS_URL=%s", os.Getenv("NATS_URL")),
		"-e", fmt.Sprintf("REDIS_URL=%s", os.Getenv("REDIS_URL")),
		"-v", fmt.Sprintf("%s:/app/configs", os.Getenv("CONFIG_DIR")),
		s.agentImage,
		"./agent",
		"-config", fmt.Sprintf("${CONFIG}"),
		"-nats", fmt.Sprintf("${NATS_URL}"),
		"-redis", fmt.Sprintf("${REDIS_URL}"))

	output, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("failed to create container: %w, output: %s", err, string(output))
	}

	containerID := strings.TrimSpace(string(output))
	
	s.mu.Lock()
	s.runningContainers[containerID] = role
	s.mu.Unlock()

	log.Printf("[%s] Started container: %s (ID: %s)", role, containerName, containerID[:12])
	return nil
}

func (s *Scaler) scaleDown(ctx context.Context, role string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Find a container for this role
	var containerID string
	for cid, r := range s.runningContainers {
		if r == role {
			containerID = cid
			break
		}
	}

	if containerID == "" {
		return fmt.Errorf("no container found for role: %s", role)
	}

	// Use Docker CLI to stop and remove container
	cmd := exec.CommandContext(ctx, "docker", "rm", "-f", containerID)
	output, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("failed to remove container: %w, output: %s", err, string(output))
	}

	delete(s.runningContainers, containerID)
	log.Printf("[%s] Stopped and removed container: %s", role, containerID[:12])
	return nil
}

func (s *Scaler) ensureMinInstances(ctx context.Context, role string) error {
	currentCount := s.getRunningContainerCount(role)
	for currentCount < s.minInstances {
		if err := s.scaleUp(ctx, role); err != nil {
			return err
		}
		currentCount++
	}
	return nil
}

func (s *Scaler) Close() {
	if s.nc != nil {
		s.nc.Close()
	}
}
