package main

import (
	"fmt"
	"log/slog"
	"os"

	"automated-trader/internal/exporter"
	"automated-trader/internal/strategy"
	"automated-trader/internal/trader"
)

func main() {
	if err := run(os.Args[1:]); err != nil {
		slog.Error("trader stopped", "error", err)
		os.Exit(1)
	}
}

func run(args []string) error {
	if len(args) == 0 {
		return trader.Run()
	}
	switch args[0] {
	case "exporter":
		return exporter.Run()
	case "backtest":
		return strategy.RunBacktestCLI(args[1:])
	case "healthcheck":
		return exporter.Healthcheck()
	default:
		return fmt.Errorf("usage: trader [exporter|backtest --input file.json|healthcheck]")
	}
}
