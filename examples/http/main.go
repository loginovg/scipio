package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"time"
)

type startResponse struct {
	ID string `json:"id"`
}

type sagaResponse struct {
	Status string `json:"status"`
}

func main() {
	if err := run(); err != nil {
		_, _ = fmt.Fprintln(os.Stderr, err.Error())
		os.Exit(1)
	}
}

func run() error {
	baseURL := os.Getenv("SCIPIO_HTTP_ADDR")
	if baseURL == "" {
		baseURL = "http://127.0.0.1:8080"
	}

	client := &http.Client{Timeout: 3 * time.Second}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	body := map[string]any{
		"workflow": "order_flow",
		"context":  map[string]any{"order_id": "A-200"},
	}

	payload, err := json.Marshal(body)
	if err != nil {
		return fmt.Errorf("marshal start request: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, baseURL+"/sagas", bytes.NewReader(payload))
	if err != nil {
		return fmt.Errorf("build start request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("start saga request failed: %w", err)
	}
	defer func() {
		if closeErr := resp.Body.Close(); closeErr != nil {
			_, _ = fmt.Fprintf(os.Stderr, "failed to close start response body: %v\n", closeErr)
		}
	}()

	if resp.StatusCode != http.StatusAccepted {
		responseBody, readErr := io.ReadAll(resp.Body)
		if readErr != nil {
			return fmt.Errorf("read start response body: %w", readErr)
		}

		return fmt.Errorf("start saga returned %d: %s", resp.StatusCode, strings.TrimSpace(string(responseBody)))
	}

	var started startResponse
	if decodeErr := json.NewDecoder(resp.Body).Decode(&started); decodeErr != nil {
		return fmt.Errorf("decode start response: %w", decodeErr)
	}

	fmt.Printf("started saga: %s\n", started.ID)

	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	for {
		getReq, reqErr := http.NewRequestWithContext(ctx, http.MethodGet, baseURL+"/sagas/"+started.ID, nil)
		if reqErr != nil {
			return fmt.Errorf("build get saga request: %w", reqErr)
		}

		sagaResp, getErr := client.Do(getReq)
		if getErr != nil {
			return fmt.Errorf("get saga request failed: %w", getErr)
		}

		if sagaResp.StatusCode != http.StatusOK {
			responseBody, readErr := io.ReadAll(sagaResp.Body)
			if closeErr := sagaResp.Body.Close(); closeErr != nil {
				return fmt.Errorf("close get saga response body: %w", closeErr)
			}
			if readErr != nil {
				return fmt.Errorf("read get saga response body: %w", readErr)
			}
			return fmt.Errorf("get saga returned %d: %s", sagaResp.StatusCode, strings.TrimSpace(string(responseBody)))
		}

		var saga sagaResponse
		if decodeErr := json.NewDecoder(sagaResp.Body).Decode(&saga); decodeErr != nil {
			if closeErr := sagaResp.Body.Close(); closeErr != nil {
				return fmt.Errorf("close get saga response body: %w", closeErr)
			}
			return fmt.Errorf("decode get saga response: %w", decodeErr)
		}
		if closeErr := sagaResp.Body.Close(); closeErr != nil {
			return fmt.Errorf("close get saga response body: %w", closeErr)
		}

		fmt.Printf("status: %s\n", saga.Status)
		if saga.Status == "COMPLETED" || saga.Status == "COMPENSATED" || saga.Status == "FAILED" {
			return nil
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
}
