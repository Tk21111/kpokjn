package alpaca

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"

	"kpokjn/internal/config"
	"kpokjn/internal/logx"
)

// Client wraps an HTTP client for the Alpaca API.
type Client struct {
	cfg    *config.Config
	client *http.Client
}

// Bar represents a single OHLCV bar from Alpaca.
type Bar struct {
	Timestamp time.Time `json:"t"`
	Open      float64   `json:"o"`
	High      float64   `json:"h"`
	Low       float64   `json:"l"`
	Close     float64   `json:"c"`
	Volume    int64     `json:"v"`
}

// BarsResponse is the Alpaca bars API response shape.
type BarsResponse struct {
	Bars          []Bar  `json:"bars"`
	NextPageToken string `json:"next_page_token"`
}

// NewClient creates a new Alpaca API client.
func NewClient(cfg *config.Config) *Client {
	return &Client{
		cfg: cfg,
		client: &http.Client{
			Timeout: 15 * time.Second,
		},
	}
}

// Do executes an HTTP request with Alpaca auth headers injected.
func (c *Client) Do(req *http.Request) (*http.Response, error) {
	req.Header.Set("APCA-API-KEY-ID", c.cfg.AlpacaAPIKey)
	req.Header.Set("APCA-API-SECRET-KEY", c.cfg.AlpacaSecret)
	return c.client.Do(req)
}

// GetBars fetches daily bars for a ticker (legacy method).
func (c *Client) GetBars(ticker string, limit int) ([]Bar, error) {
	url := fmt.Sprintf("%s/stocks/%s/bars?timeframe=1Day&adjustment=all&limit=%d&start=2023-01-01T00:00:00Z",
		c.cfg.AlpacaBaseURL, ticker, limit)

	logx.Debugf("GET %s", url)

	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return nil, fmt.Errorf("creating request: %w", err)
	}

	resp, err := c.Do(req)
	if err != nil {
		return nil, fmt.Errorf("executing request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		logx.Errorf("Alpaca API returned status %d: %s", resp.StatusCode, string(body))
		return nil, fmt.Errorf("alpaca API returned status %d: %s", resp.StatusCode, string(body))
	}

	var result BarsResponse
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, fmt.Errorf("decoding response: %w", err)
	}

	logx.Debugf("Decoded %d bars for %s", len(result.Bars), ticker)
	return result.Bars, nil
}

// GetBarsPaged fetches a page of hourly bars ending before `end`.
// Returns the bars and the next page token (empty if no more pages).
func (c *Client) GetBarsPaged(ticker, timeframe string, end time.Time, limit int) ([]Bar, string, error) {
	url := fmt.Sprintf("%s/stocks/%s/bars?timeframe=%s&adjustment=all&limit=%d&end=%s",
		c.cfg.AlpacaBaseURL, ticker, timeframe, limit, end.Format(time.RFC3339))

	logx.Debugf("GET %s", url)

	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return nil, "", fmt.Errorf("creating request: %w", err)
	}

	resp, err := c.Do(req)
	if err != nil {
		return nil, "", fmt.Errorf("executing request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		logx.Errorf("Alpaca API returned status %d: %s", resp.StatusCode, string(body))
		return nil, "", fmt.Errorf("alpaca API returned status %d: %s", resp.StatusCode, string(body))
	}

	var result BarsResponse
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, "", fmt.Errorf("decoding response: %w", err)
	}

	logx.Debugf("Decoded %d bars for %s (next_token=%q)", len(result.Bars), ticker, result.NextPageToken)
	return result.Bars, result.NextPageToken, nil
}
