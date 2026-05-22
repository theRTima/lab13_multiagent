package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"time"

	"github.com/nats-io/nats.go"
)

func main() {
	natsURL := flag.String("nats", nats.DefaultURL, "NATS server URL")
	msgType := flag.String("type", "auction", "Message type: 'task' or 'auction'")
	subject := flag.String("subject", "", "Subject to send to (required)")
	flag.Parse()

	if *subject == "" {
		log.Fatal("Subject required: -subject <subject>")
	}

	nc, err := nats.Connect(*natsURL)
	if err != nil {
		log.Fatalf("Failed to connect: %v", err)
	}
	defer nc.Close()

	traceID := fmt.Sprintf("trace-%d", time.Now().Unix())

	if *msgType == "task" {
		sendTaskMessage(nc, *subject, traceID)
	} else if *msgType == "auction" {
		sendAuctionMessage(nc, *subject, traceID)
	} else {
		log.Fatalf("Unknown message type: %s", *msgType)
	}
}

func sendTaskMessage(nc *nats.Conn, subject, traceID string) {
	msg := map[string]interface{}{
		"type":     "income.validate",
		"data":     map[string]interface{}{"applicant_id": "APP001", "annual_income": 75000},
		"trace_id": traceID,
	}
	data, _ := json.Marshal(msg)

	log.Printf("Sending task to %s (trace: %s)", subject, traceID)
	resp, err := nc.Request(subject, data, 5*time.Second)
	if err != nil {
		log.Fatalf("Request failed: %v", err)
	}

	var result map[string]interface{}
	json.Unmarshal(resp.Data, &result)
	fmt.Printf("Response:\n%s\n", prettyJSON(resp.Data))
}

func sendAuctionMessage(nc *nats.Conn, subject, traceID string) {
	msg := map[string]interface{}{
		"task_type": "income_eval",
		"trace_id":  traceID,
	}
	data, _ := json.Marshal(msg)

	log.Printf("Sending auction to %s (trace: %s)", subject, traceID)
	resp, err := nc.Request(subject, data, 5*time.Second)
	if err != nil {
		log.Fatalf("Request failed: %v", err)
	}

	fmt.Printf("Bid:\n%s\n", prettyJSON(resp.Data))
}

func prettyJSON(data []byte) string {
	var v interface{}
	json.Unmarshal(data, &v)
	pretty, _ := json.MarshalIndent(v, "", "  ")
	return string(pretty)
}
