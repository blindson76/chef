package runtime

import (
    "context"
    "net"
    "net/http"
    "time"
)

func ListenAndServe(ctx context.Context, addr string, h http.Handler) error {
    srv := &http.Server{
        Addr: addr,
        Handler: h,
        ReadHeaderTimeout: 5*time.Second,
    }
    ln, err := net.Listen("tcp", addr)
    if err != nil {
        return err
    }
    errCh := make(chan error, 1)
    go func() { errCh <- srv.Serve(ln) }()
    select {
    case <-ctx.Done():
        _ = srv.Shutdown(context.Background())
        return ctx.Err()
    case err := <-errCh:
        return err
    }
}
