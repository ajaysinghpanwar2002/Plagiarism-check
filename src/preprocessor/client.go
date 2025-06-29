package preprocessor

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"

	"plagiarism-detector/src/monitoring"

	"github.com/cactus/go-statsd-client/v5/statsd"
)

type PreprocessRequest struct {
	Text     string `json:"text"`
	Language string `json:"language"`
}

type PreprocessResponse struct {
	OriginalText  string `json:"original_text"`
	ProcessedText string `json:"processed_text"`
	Language      string `json:"language"`
}

type Client struct {
	httpClient *http.Client
	baseURL    string
	statsd     statsd.Statter
}

func NewClient(serviceURL string, statsdClient statsd.Statter) *Client {
	return &Client{
		httpClient: &http.Client{
			Timeout: 30 * time.Second,
		},
		baseURL: serviceURL,
		statsd:  statsdClient,
	}
}

func (c *Client) PreprocessText(ctx context.Context, text, language string) (string, error) {
	reqBody := PreprocessRequest{
		Text:     text,
		Language: language,
	}

	payload, err := json.Marshal(reqBody)
	if err != nil {
		monitoring.Increment("preprocess.api.marshal.failed", c.statsd)
		return "", fmt.Errorf("failed to marshal request body: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, "POST", c.baseURL+"/preprocess", bytes.NewBuffer(payload))
	if err != nil {
		monitoring.Increment("preprocess.api.request.failed", c.statsd)
		return "", fmt.Errorf("failed to create preprocess request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json")

	resp, err := c.httpClient.Do(req)
	if err != nil {
		monitoring.Increment("preprocess.api.network.error", c.statsd)
		return "", fmt.Errorf("error calling preprocessor service: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		bodyBytes, _ := io.ReadAll(resp.Body)
		monitoring.Increment(fmt.Sprintf("preprocess.api.status_code.%d", resp.StatusCode), c.statsd)
		return "", fmt.Errorf("preprocessor service returned non-200 status: %d, body: %s", resp.StatusCode, string(bodyBytes))
	}

	var preprocessResp PreprocessResponse
	if err := json.NewDecoder(resp.Body).Decode(&preprocessResp); err != nil {
		monitoring.Increment("preprocess.api.unmarshal.failed", c.statsd)
		return "", fmt.Errorf("failed to decode response body: %w", err)
	}

	monitoring.Increment("preprocess.api.success", c.statsd)
	return preprocessResp.ProcessedText, nil
}
