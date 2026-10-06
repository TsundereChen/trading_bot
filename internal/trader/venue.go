package trader

import (
	"fmt"
	"strings"
)

// Venue describes where orders and market data come from.
type Venue struct {
	Name string // "demo" or "live"
	// BaseURL is the REST base for market data and signed endpoints.
	BaseURL string
	// StreamURL is the WebSocket combined-stream base.
	StreamURL string
	Live      bool
}

const (
	demoBaseURL   = "https://demo-api.binance.com"
	demoStreamURL = "wss://demo-stream.binance.com"
	liveBaseURL   = "https://api.binance.com"
	liveStreamURL = "wss://stream.binance.com:9443"
)

// resolveVenue maps configuration to a venue.
//
// An unset BINANCE_BASE_URL selects Demo Mode. Naming the production host
// explicitly selects live; ENABLE_TRADING remains the only switch that permits
// orders, on either venue. Live is the operator's risk to accept and is
// reported loudly at startup rather than gated here.
func resolveVenue(baseURL string) (Venue, error) {
	base := strings.TrimRight(strings.TrimSpace(baseURL), "/")
	// The demo documentation includes /api in its base URL; request paths
	// already include /api/v3, so accept either spelling without duplicating it.
	base = strings.TrimSuffix(base, "/api")
	switch base {
	case "":
		return demo(), nil
	case demoBaseURL:
		return demo(), nil
	case liveBaseURL:
		return Venue{Name: "live", BaseURL: liveBaseURL, StreamURL: liveStreamURL, Live: true}, nil
	default:
		return Venue{}, fmt.Errorf("unsupported BINANCE_BASE_URL %q: use %s for demo or %s for live", base, demoBaseURL, liveBaseURL)
	}
}

func demo() Venue {
	return Venue{Name: "demo", BaseURL: demoBaseURL, StreamURL: demoStreamURL}
}

func (v Venue) label() string { return v.Name }
