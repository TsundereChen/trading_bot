package trader

import "time"

func signalInterval(value string) string {
	if value == "" {
		return "1m"
	}
	return value
}

func candleDuration(value string) time.Duration {
	if signalInterval(value) == "5m" {
		return 5 * time.Minute
	}
	return time.Minute
}
