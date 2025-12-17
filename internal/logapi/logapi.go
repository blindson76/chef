package logapi

import (
    "bufio"
    "encoding/json"
    "fmt"
    "io"
    "net/http"
    "os"
    "strconv"
    "strings"
    "time"

    "github.com/umitbozkurt/orchestrator/internal/procman"
)

type Server struct {
    mgr *procman.Manager
    authToken string
}

func New(mgr *procman.Manager, authToken string) *Server {
    return &Server{mgr: mgr, authToken: authToken}
}

func (s *Server) Handler() http.Handler {
    mux := http.NewServeMux()
    mux.HandleFunc("/api/v1/logs/list", s.auth(s.handleList))
    mux.HandleFunc("/api/v1/logs/read", s.auth(s.handleRead))
    mux.HandleFunc("/api/v1/logs/follow", s.auth(s.handleFollow))
    mux.HandleFunc("/api/v1/logs/download", s.auth(s.handleDownload))
    mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(200); _, _ = w.Write([]byte("ok")) })
    return mux
}

func (s *Server) auth(next http.HandlerFunc) http.HandlerFunc {
    return func(w http.ResponseWriter, r *http.Request) {
        if strings.TrimSpace(s.authToken) == "" {
            // allow if not configured
            next(w, r); return
        }
        tok := r.Header.Get("Authorization")
        if tok == "" {
            tok = r.URL.Query().Get("token")
        }
        tok = strings.TrimPrefix(tok, "Bearer ")
        if tok != s.authToken {
            w.WriteHeader(http.StatusUnauthorized)
            _, _ = w.Write([]byte("unauthorized"))
            return
        }
        next(w, r)
    }
}

func (s *Server) handleList(w http.ResponseWriter, r *http.Request) {
    type item struct {
        ID string `json:"id"`
        Service string `json:"service"`
        StartedAt time.Time `json:"startedAt"`
        Cmdline []string `json:"cmdline"`
        Exited bool `json:"exited"`
        Error string `json:"error,omitempty"`
    }
    items := []item{}
    for _, inst := range s.mgr.List() {
        exited := false
        err := inst.ExitError()
        if err != nil {
            exited = true
        }
        items = append(items, item{
            ID: inst.ID,
            Service: inst.Service,
            StartedAt: inst.StartedAt,
            Cmdline: inst.Cmdline,
            Exited: exited,
            Error: errString(err),
        })
    }
    _ = json.NewEncoder(w).Encode(items)
}

func (s *Server) handleRead(w http.ResponseWriter, r *http.Request) {
    id := r.URL.Query().Get("instanceId")
    stream := r.URL.Query().Get("stream")
    if stream == "" { stream = "combined" }
    offset, _ := strconv.ParseInt(r.URL.Query().Get("offset"), 10, 64)
    limit, _ := strconv.ParseInt(r.URL.Query().Get("limit"), 10, 64)
    if limit <= 0 || limit > 4*1024*1024 {
        limit = 512 * 1024
    }
    inst := s.mgr.Get(id)
    if inst == nil {
        http.NotFound(w, r); return
    }
    path := pickPath(inst, stream)
    f, err := os.Open(path)
    if err != nil { http.Error(w, err.Error(), 500); return }
    defer f.Close()

    if offset > 0 {
        _, _ = f.Seek(offset, io.SeekStart)
    }
    buf := make([]byte, limit)
    n, _ := f.Read(buf)
    resp := map[string]any{
        "instanceId": id,
        "stream": stream,
        "offset": offset,
        "nextOffset": offset + int64(n),
        "data": string(buf[:n]),
    }
    _ = json.NewEncoder(w).Encode(resp)
}

func (s *Server) handleDownload(w http.ResponseWriter, r *http.Request) {
    id := r.URL.Query().Get("instanceId")
    stream := r.URL.Query().Get("stream")
    if stream == "" { stream = "combined" }
    inst := s.mgr.Get(id)
    if inst == nil { http.NotFound(w,r); return }
    path := pickPath(inst, stream)
    w.Header().Set("Content-Type", "text/plain; charset=utf-8")
    w.Header().Set("Content-Disposition", fmt.Sprintf("attachment; filename=%s-%s.log", id, stream))
    http.ServeFile(w, r, path)
}

// SSE follow (best-effort): polls for growth.
func (s *Server) handleFollow(w http.ResponseWriter, r *http.Request) {
    id := r.URL.Query().Get("instanceId")
    stream := r.URL.Query().Get("stream")
    if stream == "" { stream = "combined" }
    inst := s.mgr.Get(id)
    if inst == nil { http.NotFound(w,r); return }
    path := pickPath(inst, stream)

    flusher, ok := w.(http.Flusher)
    if !ok { http.Error(w, "no flusher", 500); return }

    w.Header().Set("Content-Type", "text/event-stream")
    w.Header().Set("Cache-Control", "no-cache")
    w.Header().Set("Connection", "keep-alive")

    var offset int64 = 0
    if off := r.URL.Query().Get("offset"); off != "" {
        if v, err := strconv.ParseInt(off, 10, 64); err == nil { offset = v }
    }

    ticker := time.NewTicker(500 * time.Millisecond)
    defer ticker.Stop()

    for {
        select {
        case <-r.Context().Done():
            return
        case <-ticker.C:
            fi, err := os.Stat(path)
            if err != nil { continue }
            if fi.Size() <= offset { continue }

            f, err := os.Open(path)
            if err != nil { continue }
            _, _ = f.Seek(offset, io.SeekStart)
            rd := bufio.NewReader(f)
            for {
                line, err := rd.ReadString('\n')
                if len(line) > 0 {
                    offset += int64(len(line))
                    fmt.Fprintf(w, "data: %s\n\n", strings.TrimRight(line, "\r\n"))
                    flusher.Flush()
                }
                if err != nil {
                    break
                }
            }
            _ = f.Close()
        }
    }
}

func pickPath(inst *procman.Instance, stream string) string {
    so, se, co := inst.LogPaths()
    switch strings.ToLower(stream) {
    case "stdout":
        return so
    case "stderr":
        return se
    default:
        return co
    }
}

func errString(err error) string {
    if err == nil { return "" }
    return err.Error()
}
