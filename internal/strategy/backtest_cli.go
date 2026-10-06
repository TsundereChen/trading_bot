package strategy

import (
	"context"
	"encoding/json"
	"flag"
	"io"
	"os"
)

// RunBacktestCLI reads JSON input, runs a rule-only backtest, and writes JSON.
func RunBacktestCLI(args []string) error {
	flags := flag.NewFlagSet("backtest", flag.ContinueOnError)
	path := flags.String("input", "-", "JSON input path or - for stdin")
	if err := flags.Parse(args); err != nil {
		return err
	}
	var reader io.Reader = os.Stdin
	if *path != "-" {
		f, err := os.Open(*path)
		if err != nil {
			return err
		}
		defer f.Close()
		reader = f
	}
	decoder := json.NewDecoder(io.LimitReader(reader, 8<<20))
	decoder.DisallowUnknownFields()
	var input BacktestInput
	if err := decoder.Decode(&input); err != nil {
		return err
	}
	result, err := Backtest(context.Background(), input)
	if err != nil {
		return err
	}
	encoder := json.NewEncoder(os.Stdout)
	encoder.SetIndent("", "  ")
	return encoder.Encode(result)
}
