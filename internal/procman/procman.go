package procman

import (
    "bufio"
    "context"
    "errors"
    "fmt"
    "io"
    "os"
    "os/exec"
    "path/filepath"
    "sync"
    "time"
)

type StopMode string

const (
    StopTerminate StopMode = "terminate"
    StopInterrupt StopMode = "interrupt"
)

type Instance struct {
    ID        string
    Service   string
    StartedAt time.Time
    Cmdline   []string

    cmd   *exec.Cmd
    mu    sync.Mutex
    done  chan struct{}
    err   error

    stdoutPath string
    stderrPath string
    combinedPath string
}

type Manager struct {
    logRoot string
    mu sync.Mutex
    instances map[string]*Instance
}

func New(logRoot string) *Manager {
    return &Manager{logRoot: logRoot, instances: map[string]*Instance{}}
}

func (m *Manager) Start(ctx context.Context, service, instanceID string, exe string, args []string, workdir string) (*Instance, error) {
    if exe == "" {
        return nil, errors.New("exe required")
    }
    inst := &Instance{
        ID: instanceID,
        Service: service,
        StartedAt: time.Now(),
        Cmdline: append([]string{exe}, args...),
        done: make(chan struct{}),
    }

    dir := filepath.Join(m.logRoot, service, instanceID)
    if err := os.MkdirAll(dir, 0o755); err != nil {
        return nil, err
    }
    inst.stdoutPath = filepath.Join(dir, "stdout.log")
    inst.stderrPath = filepath.Join(dir, "stderr.log")
    inst.combinedPath = filepath.Join(dir, "combined.log")

    stdoutF, err := os.OpenFile(inst.stdoutPath, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
    if err != nil { return nil, err }
    stderrF, err := os.OpenFile(inst.stderrPath, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
    if err != nil { _ = stdoutF.Close(); return nil, err }
    combinedF, err := os.OpenFile(inst.combinedPath, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
    if err != nil { _ = stdoutF.Close(); _ = stderrF.Close(); return nil, err }

    cmd := exec.CommandContext(ctx, exe, args...)
    if workdir != "" {
        cmd.Dir = workdir
    }
    // inherit environment by default
    so, err := cmd.StdoutPipe()
    if err != nil { return nil, err }
    se, err := cmd.StderrPipe()
    if err != nil { return nil, err }

    if err := cmd.Start(); err != nil {
        return nil, err
    }
    inst.cmd = cmd

    go func() {
        defer close(inst.done)
        // tee stdout/stderr into their files + combined
        var wg sync.WaitGroup
        wg.Add(2)
        go func() { defer wg.Done(); pipeToFiles(so, stdoutF, combinedF) }()
        go func() { defer wg.Done(); pipeToFiles(se, stderrF, combinedF) }()
        wg.Wait()
        _ = stdoutF.Close()
        _ = stderrF.Close()
        _ = combinedF.Close()
        inst.mu.Lock()
        inst.err = cmd.Wait()
        inst.mu.Unlock()
    }()

    m.mu.Lock()
    m.instances[inst.ID] = inst
    m.mu.Unlock()
    return inst, nil
}

func pipeToFiles(r io.Reader, f1 *os.File, combined *os.File) {
    sc := bufio.NewScanner(r)
    for sc.Scan() {
        line := sc.Text()
        _, _ = fmt.Fprintln(f1, line)
        _, _ = fmt.Fprintln(combined, line)
    }
}

func (m *Manager) Stop(ctx context.Context, id string, mode StopMode) error {
    m.mu.Lock()
    inst := m.instances[id]
    m.mu.Unlock()
    if inst == nil || inst.cmd == nil || inst.cmd.Process == nil {
        return errors.New("process not found")
    }
    switch mode {
    case StopInterrupt:
        _ = inst.cmd.Process.Signal(os.Interrupt)
    default:
        _ = inst.cmd.Process.Kill()
    }
    select {
    case <-ctx.Done():
        return ctx.Err()
    case <-inst.done:
        return nil
    case <-time.After(10 * time.Second):
        _ = inst.cmd.Process.Kill()
        return errors.New("stop timeout; killed")
    }
}

func (m *Manager) List() []*Instance {
    m.mu.Lock()
    defer m.mu.Unlock()
    out := make([]*Instance, 0, len(m.instances))
    for _, v := range m.instances {
        out = append(out, v)
    }
    return out
}

func (m *Manager) Get(id string) *Instance {
    m.mu.Lock()
    defer m.mu.Unlock()
    return m.instances[id]
}

func (i *Instance) ExitError() error {
    i.mu.Lock()
    defer i.mu.Unlock()
    return i.err
}

func (i *Instance) LogPaths() (stdout, stderr, combined string) {
    return i.stdoutPath, i.stderrPath, i.combinedPath
}
