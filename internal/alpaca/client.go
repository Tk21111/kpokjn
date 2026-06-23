package alpaca

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"time"

	"kpokjn/domain"
	"kpokjn/internal/config"
	"kpokjn/internal/logx"
)

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

func NewClient(cfg *config.Config) *domain.Client {
	return &domain.Client{
		Cfg: cfg,
		Client: &http.Client{
			Timeout: 15 * time.Second,
		},
	}
}

// Do executes an HTTP request with Alpaca auth headers injected.
func Do(c *domain.Client, req *http.Request) (*http.Response, error) {
	req.Header.Set("APCA-API-KEY-ID", c.Cfg.AlpacaAPIKey)
	req.Header.Set("APCA-API-SECRET-KEY", c.Cfg.AlpacaSecret)
	return c.Client.Do(req)
}

// GetBarsPaged fetches a page of hourly bars ending before `end`.
// Returns the bars and the next page token (empty if no more pages).
func GetAllBars(c *domain.Client, apiJob *domain.ApiJob) ([]Bar, string, error) {
	var allBars []Bar

	urlStr := fmt.Sprintf("%s/stocks/%s/bars?timeframe=%s&adjustment=all&limit=%d&start=%s",
		c.Cfg.AlpacaBaseURL, apiJob.Ticker, apiJob.TimeFrame, apiJob.Limit, apiJob.Start.UTC().Format(time.RFC3339))

	if !apiJob.End.IsZero() {
		urlStr = fmt.Sprintf("%s&end=%s", urlStr, apiJob.End.UTC().Format(time.RFC3339))
	}

	if apiJob.NextPageToken != "" {
		urlStr = fmt.Sprintf("%s&page_token=%s", urlStr, url.QueryEscape(apiJob.NextPageToken))
	}

	logx.Debugf("GET %s", urlStr)
	req, err := http.NewRequest("GET", urlStr, nil)
	if err != nil {
		return nil, "", fmt.Errorf("creating request: %w", err)
	}

	resp, err := Do(c, req)
	if err != nil {
		return nil, "", fmt.Errorf("executing request: %w", err)
	}

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		logx.Errorf("Alpaca API returned status %d: %s", resp.StatusCode, string(body))
		return nil, "", fmt.Errorf("alpaca API returned status %d: %s", resp.StatusCode, string(body))
	}

	var result BarsResponse
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		resp.Body.Close()
		return nil, "", fmt.Errorf("decoding response: %w", err)
	}

	resp.Body.Close()

	allBars = append(allBars, result.Bars...)
	logx.Debugf("Fetched %d bars for %s. Total so far: %d", len(result.Bars), apiJob.Ticker, len(allBars))

	logx.Debugf("Finished fetching all %d bars for %s", len(allBars), apiJob.Ticker)
	return allBars, result.NextPageToken, nil
}
