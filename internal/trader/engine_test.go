package trader

import (
	"context"
	"reflect"
	"testing"
	"time"
)

func TestDecisionPairsConfigurationOrderAndRetainedPairs(t *testing.T) {
	a := testApp(t)
	a.cfg.Pairs = []string{"ETHUSDT", "BTCUSDT"}
	a.state.Pairs["ETHUSDT"] = &Position{Pair: "ETHUSDT"}
	a.state.Pairs["BTCUSDC"] = &Position{Pair: "BTCUSDC"}
	want := []string{"ETHUSDT", "BTCUSDT", "BTCUSDC"}
	if got := a.decisionPairs(); !reflect.DeepEqual(got, want) {
		t.Fatalf("pairs = %v, want %v", got, want)
	}
}

func TestDecisionRoundEvaluatesEveryPairWithoutInterPairWait(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	pairs := []string{"BTCUSDT", "BTCUSDC", "ETHUSDT", "ETHUSDC"}
	var got []string
	var first, last time.Time
	// Cancel during the first round so a one-pair-per-interval implementation
	// cannot pass by accumulating four calls across four separate rounds.
	runDecisionRounds(ctx, 100*time.Millisecond, func() []string { return pairs }, func(_ context.Context, pair string) {
		if len(got) == 0 {
			first = time.Now()
		}
		got = append(got, pair)
		last = time.Now()
		if len(got) == len(pairs) {
			cancel()
		}
	})
	if !reflect.DeepEqual(got, pairs) {
		t.Fatalf("round = %v, want %v", got, pairs)
	}
	if last.Sub(first) >= 100*time.Millisecond {
		t.Fatal("scheduler waited an interval between pairs")
	}
}

func TestDecisionRoundsSerializeOverrunsAndRefreshPairs(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	round := 0
	var got []string
	active := false
	runDecisionRounds(ctx, time.Millisecond, func() []string {
		round++
		if round == 1 {
			return []string{"BTCUSDT", "ETHUSDT"}
		}
		return []string{"BTCUSDC"}
	}, func(_ context.Context, pair string) {
		if active {
			t.Fatal("overlapping evaluations")
		}
		active = true
		time.Sleep(5 * time.Millisecond)
		got = append(got, pair)
		active = false
		if len(got) == 3 {
			cancel()
		}
	})
	want := []string{"BTCUSDT", "ETHUSDT", "BTCUSDC"}
	if !reflect.DeepEqual(got, want) || round != 2 {
		t.Fatalf("calls = %v, rounds = %d", got, round)
	}
}

func TestDecisionRoundCancellationStopsRemainingPairs(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	calls := 0
	runDecisionRounds(ctx, time.Millisecond, func() []string { return []string{"BTCUSDT", "ETHUSDT"} }, func(_ context.Context, _ string) {
		calls++
		cancel()
	})
	if calls != 1 {
		t.Fatalf("evaluated %d pairs after cancellation", calls)
	}
}
