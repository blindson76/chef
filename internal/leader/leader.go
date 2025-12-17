package leader

import (
	"context"
	"log"
	"time"

	"github.com/umitbozkurt/orchestrator/internal/store"
)

type Runner func(ctx context.Context) error

func RunWithLock(ctx context.Context, st store.Store, lockKey string, ttl time.Duration, run Runner) error {
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
		lh, err := st.Lock(ctx, lockKey, ttl)
		if err != nil {
			time.Sleep(500 * time.Millisecond)
			continue
		}
		if !lh.IsHeld() {
			time.Sleep(750 * time.Millisecond)
			continue
		}

		lockCtx, cancel := context.WithCancel(ctx)
		errCh := make(chan error, 1)
		go func() { errCh <- lh.KeepAlive(lockCtx) }()
		runCh := make(chan error, 1)
		go func() { runCh <- run(lockCtx) }()

		select {
		case err := <-errCh:
			cancel()
			_ = lh.Unlock(context.Background())
			log.Printf("[leader] lost lock %s: %v", lockKey, err)
		case err := <-runCh:
			cancel()
			_ = lh.Unlock(context.Background())
			return err
		case <-ctx.Done():
			cancel()
			_ = lh.Unlock(context.Background())
			return ctx.Err()
		}
	}
}
