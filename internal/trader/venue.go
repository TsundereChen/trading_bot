package trader

import (
	"fmt"
	"strings"
)

// Venue describes where orders and market data come from.
type Venue struct {
	Name string // "paper" or "live"
	// BaseURL is the REST base for market data and signed endpoints.
	BaseURL string
	// StreamURL is the WebSocket combined-stream base.
	StreamURL string
	Live      bool
}

const (
	paperBaseURL   = "https://testnet.binance.vision"
	paperStreamURL = "wss://stream.testnet.binance.vision"
	liveBaseURL    = "https://api.binance.com"
	liveStreamURL  = "wss://stream.binance.com:9443"
)

// resolveVenue maps configuration to a venue.
//
// An unset BINANCE_BASE_URL selects testnet. Naming the production host
// explicitly selects live; ENABLE_TRADING remains the only switch that permits
// orders, on either venue. Live is the operator's risk to accept and is
// reported loudly at startup rather than gated here.
func resolveVenue(baseURL string) (Venue, error) {
	base := strings.TrimRight(strings.TrimSpace(baseURL), "/")
	switch base {
	case "":
		return paper(), nil
	case paperBaseURL:
		return paper(), nil
	case liveBaseURL:
		return Venue{Name: "live", BaseURL: liveBaseURL, StreamURL: liveStreamURL, Live: true}, nil
	default:
		return Venue{}, fmt.Errorf("unsupported BINANCE_BASE_URL %q: use %s for testnet or %s for live", base, paperBaseURL, liveBaseURL)
	}
}

func paper() Venue {
	return Venue{Name: "paper", BaseURL: paperBaseURL, StreamURL: paperStreamURL}
}

func (v Venue) label() string { return v.Name }
