package worker

import (
	"context"
	"time"
)

func runPeriodic(ctx context.Context, interval time.Duration, runAtStart bool, run func(context.Context)) error {
	if runAtStart {
		run(ctx)
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			run(ctx)
		}
	}
}
