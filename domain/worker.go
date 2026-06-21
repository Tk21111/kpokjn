package domain

import (
	"kpokjn/internal/alpaca"
	"kpokjn/internal/data"
)

type Worker struct {
	Cfg    *ApiJob
	Client *alpaca.Client
	Writer *data.Writer
}
