# Trading Indicator Notifier - TODO

## Phase 1: Testing with Discord Bot

### 1.1 Project Setup
- [/] Initialize Go project module
- [/] Set up project directory structure
- [/] Install dependencies (SQLite driver, HTTP client, etc.)
- [/] Configure environment variables (Alpaca API keys, Discord webhook URL)

### 1.2 Alpaca API Integration
- [/] Implement Alpaca API client for fetching 1-hour adjusted candles
- [/] Fetch Tesla (TSLA) stock data only
- [/] Parse and validate OHLCV response

### 1.3 Simple Indicator
- [/] Implement a basic indicator algorithm (e.g., SMA crossover or RSI)
- [/] Pass Tesla data array into the indicator
- [/] Print indicator output to console

### 1.4 Discord Bot Notification
- [ ] Set up Discord webhook integration
- [ ] Format alert payload as JSON (ticker, indicator value, timestamp)
- [ ] Send notification to Discord when indicator threshold is hit
- [ ] Verify message appears correctly in Discord channel

---

## Phase 2: Scaling to Multiple Stocks

### 2.1 SQLite Integration
- [ ] Set up SQLite database with WAL journal mode
- [ ] Design schema for storing candle data (ticker, timestamp, OHLCV)
- [ ] Implement write path: append new hourly candles
- [ ] Implement read path: fetch lookback window per ticker

### 2.2 Worker Pool & Concurrency
- [ ] Implement Go worker pool with configurable goroutine count (1-5)
- [ ] Set up job queue for ticker processing
- [ ] Add rate limiting to stay under Alpaca 200 req/min
- [ ] Distribute requests sequentially across workers

### 2.3 Multi-Stock Support
- [ ] Expand from Tesla to 50+ stock tickers
- [ ] Add ticker list configuration (JSON/YAML config file)
- [ ] Implement per-stock lookback cascade:
  - Stock-level override
  - Indicator-level requirement
  - Global default (up to 2 years)
- [ ] Run indicator evaluation per ticker after data fetch

### 2.4 Scheduling & Automation
- [ ] Implement hourly scheduler (`time.Ticker` at top of hour)
- [ ] Enqueue all tickers on each tick
- [ ] Ensure graceful shutdown and error handling
- [ ] Add logging for monitoring and debugging

### 2.5 Alerting & Polish
- [ ] Send Discord alerts for any ticker hitting threshold
- [ ] Include ticker name, indicator value, and signal type in alert
- [ ] Handle edge cases (API failures, missing data, rate limits)
- [ ] End-to-end test with full ticker list
