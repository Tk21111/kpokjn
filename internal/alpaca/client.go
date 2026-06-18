package alpaca

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
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

type BarsResponse struct {
	Bars          []Bar  `json:"bars"`
	NextPageToken string `json:"next_page_token"`
}

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

// GetBarsPaged fetches a page of hourly bars ending before `end`.
// Returns the bars and the next page token (empty if no more pages).
func (c *Client) GetAllBars(ticker, timeframe string, start time.Time, end time.Time, limit int) ([]Bar, error) {
	var allBars []Bar
	var pageToken string

	for {
		urlStr := fmt.Sprintf("%s/stocks/%s/bars?timeframe=%s&adjustment=all&limit=%d&start=%s&end=%s",
			c.cfg.AlpacaBaseURL, ticker, timeframe, limit, start.Format(time.RFC3339), end.Format(time.RFC3339))

		if pageToken != "" {
			urlStr = fmt.Sprintf("%s&page_token=%s", urlStr, url.QueryEscape(pageToken))
		}

		logx.Debugf("GET %s", urlStr)
		req, err := http.NewRequest("GET", urlStr, nil)
		if err != nil {
			return nil, fmt.Errorf("creating request: %w", err)
		}

		resp, err := c.Do(req)
		if err != nil {
			return nil, fmt.Errorf("executing request: %w", err)
		}

		if resp.StatusCode != http.StatusOK {
			body, _ := io.ReadAll(resp.Body)
			resp.Body.Close()
			logx.Errorf("Alpaca API returned status %d: %s", resp.StatusCode, string(body))
			return nil, fmt.Errorf("alpaca API returned status %d: %s", resp.StatusCode, string(body))
		}

		var result BarsResponse
		if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
			resp.Body.Close()
			return nil, fmt.Errorf("decoding response: %w", err)
		}

		resp.Body.Close()

		allBars = append(allBars, result.Bars...)
		logx.Debugf("Fetched %d bars for %s. Total so far: %d", len(result.Bars), ticker, len(allBars))

		pageToken = result.NextPageToken
		if pageToken == "" {
			break
		}
	}

	logx.Debugf("Finished fetching all %d bars for %s", len(allBars), ticker)
	return allBars, nil
}
