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

type Client struct {
	cfg    *config.Config
	client *http.Client
}

type Bar struct {
	Timestamp time.Time `json:"t"`
	Open      float64   `json:"o"`
	High      float64   `json:"h"`
	Low       float64   `json:"l"`
	Close     float64   `json:"c"`
	Volume    int64     `json:"v"`
}

type barsResponse struct {
	Bars []Bar `json:"bars"`
}

func NewClient(cfg *config.Config) *Client {
	return &Client{
		cfg: cfg,
		client: &http.Client{
			Timeout: 15 * time.Second,
		},
	}
}

func (c *Client) GetHourlyBars(ticker string, limit int) ([]Bar, error) {
	url := fmt.Sprintf("%s/stocks/%s/bars?timeframe=1Hour&limit=%d&adjustment=all",
		c.cfg.AlpacaBaseURL, ticker, limit)

	logx.Debugf("GET %s", url)

	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return nil, fmt.Errorf("creating request: %w", err)
	}

	req.Header.Set("APCA-API-KEY-ID", c.cfg.AlpacaAPIKey)
	req.Header.Set("APCA-API-SECRET-KEY", c.cfg.AlpacaSecret)

	resp, err := c.client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("executing request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		logx.Errorf("Alpaca API returned status %d: %s", resp.StatusCode, string(body))
		return nil, fmt.Errorf("alpaca API returned status %d: %s", resp.StatusCode, string(body))
	}

	var result barsResponse
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, fmt.Errorf("decoding response: %w", err)
	}

	logx.Debugf("Decoded %d bars for %s", len(result.Bars), ticker)
	return result.Bars, nil
}
