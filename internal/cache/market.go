package cache

import (
	"fmt"
	"kpokjn/domain"
	"kpokjn/internal/data"
	"time"

	"github.com/dgraph-io/ristretto/v2"
)

type MarketCache struct {
	cache *ristretto.Cache[string, domain.Bar]
	db    *data.Writer
}

func NewMarketCache(db *data.Writer) (*MarketCache, error) {
	cache, err := ristretto.NewCache(&ristretto.Config[string, domain.Bar]{
		NumCounters: 1e7,
		MaxCost:     1000000,
		BufferItems: 64,
	})
	if err != nil {
		return nil, err
	}

	return &MarketCache{
		cache: cache,
		db:    db,
	}, nil
}

func makeKey(ticker string, timestamp int64) string {
	return fmt.Sprintf("%s:%d", ticker, timestamp)
}

func (m *MarketCache) Add(bar domain.Bar, ticker string) {
	key := makeKey(ticker, bar.Timestamp.Unix())
	m.cache.Set(key, bar, 1)
}

func (m *MarketCache) AddRows(bars []domain.Bar, ticker string) {
	for _, bar := range bars {
		m.Add(bar, ticker)
	}
}

// try to find in cache if not -> find in sqlite
func (m *MarketCache) GetFrom(ticker string, from time.Time, to time.Time, interval time.Duration) []domain.Bar {
	var results []domain.Bar

	current := from.Truncate(interval)
	end := to.Truncate(interval)

	for !current.After(end) {
		ts := current.Unix()
		key := makeKey(ticker, ts)

		if bar, found := m.cache.Get(key); found {
			// Cache Hit
			results = append(results, bar)
		} else {
			// Cache Miss - Query the SQLite database
			// yeah we can make this better but yeah no
			row := m.db.QueryRow(
				`SELECT open, high, low, close, volume 
				 FROM ohlcv 
				 WHERE ticker = ? AND timestamp = ?`,
				ticker, ts,
			)

			var b domain.Bar

			err := row.Scan(&b.Open, &b.High, &b.Low, &b.Close, &b.Volume)

			if err == nil {
				results = append(results, b)

				// note - ristretto will handle it
				m.Add(b, ticker)
			}
			// If err == sql.ErrNoRows, it means the bar doesn't exist in memory OR the database.
			// The loop simply moves on to the next timestamp.
		}

		current = current.Add(interval)
	}

	return results
}

// TODO- Gemini - don't understand what i want will do this someday ig
// func (m *MarketCache) GetFrom(ticker string, from time.Time, to time.Time, interval time.Duration) []domain.OHLCV {
// 	current := from.Truncate(interval)
// 	end := to.Truncate(interval)

// 	barsMap := make(map[int64]domain.OHLCV)
// 	var missing []int64

// 	for !current.After(end) {
// 		ts := current.Unix()
// 		key := makeKey(ticker, ts)

// 		if bar, found := m.cache.Get(key); found {
// 			barsMap[ts] = bar
// 		} else {
// 			missing = append(missing, ts)
// 		}

// 		current = current.Add(interval)
// 	}

// 	if len(missing) > 0 {
// 		minTs := missing[0]
// 		maxTs := missing[len(missing)-1]

// 		rows, err := m.db.Query(
// 			`SELECT timestamp, open, high, low, close, volume
// 			 FROM ohlcv
// 			 WHERE ticker = ? AND timestamp >= ? AND timestamp <= ?`,
// 			ticker, minTs, maxTs,
// 		)

// 		if err == nil {
// 			defer rows.Close()
// 			for rows.Next() {
// 				var b domain.OHLCV
// 				b.Ticker = ticker
// 				if err := rows.Scan(&b.Timestamp, &b.Open, &b.High, &b.Low, &b.Close, &b.Volume); err == nil {
// 					if _, exists := barsMap[b.Timestamp]; !exists {
// 						barsMap[b.Timestamp] = b
// 						m.Add(b)
// 					}
// 				}
// 			}
// 		}
// 	}

// 	var results []domain.OHLCV
// 	current = from.Truncate(interval)

// 	for !current.After(end) {
// 		if bar, ok := barsMap[current.Unix()]; ok {
// 			results = append(results, bar)
// 		}
// 		current = current.Add(interval)
// 	}

// 	return results
// }
