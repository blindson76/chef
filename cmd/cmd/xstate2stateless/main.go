package main

import (
	"flag"
	"fmt"
	"os"
	"path/filepath"

	"github.com/umitbozkurt/orchestrator/internal/gen"
	"github.com/umitbozkurt/orchestrator/internal/xstate"
)

func main() {
	in := flag.String("in", "", "Path to XState machine JSON (config object)")
	out := flag.String("out", "", "Output .go file path (default: <in>_stateless.go)")
	pkg := flag.String("pkg", "machine", "Generated Go package name")
	name := flag.String("name", "", "Generated type name (default: derived from id or file name)")
	flag.Parse()

	if *in == "" {
		fmt.Fprintln(os.Stderr, "error: -in is required")
		os.Exit(2)
	}

	b, err := os.ReadFile(*in)
	if err != nil {
		fmt.Fprintln(os.Stderr, "read:", err)
		os.Exit(1)
	}

	m, err := xstate.ParseMachineJSON(b)
	if err != nil {
		fmt.Fprintln(os.Stderr, "parse:", err)
		os.Exit(1)
	}

	outPath := *out
	if outPath == "" {
		ext := filepath.Ext(*in)
		base := (*in)[:len(*in)-len(ext)]
		outPath = base + "_stateless.go"
	}

	typeName := *name
	if typeName == "" {
		typeName = gen.DefaultTypeName(m.ID, *in)
	}

	code, err := gen.Generate(gen.Options{
		PackageName: *pkg,
		TypeName:    typeName,
		Machine:     m,
		SourcePath:  *in,
	})
	if err != nil {
		fmt.Fprintln(os.Stderr, "generate:", err)
		os.Exit(1)
	}

	if dir := filepath.Dir(outPath); dir != "." && dir != "" {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			fmt.Fprintln(os.Stderr, "mkdir:", err)
			os.Exit(1)
		}
	}

	if err := os.WriteFile(outPath, code, 0o644); err != nil {
		fmt.Fprintln(os.Stderr, "write:", err)
		os.Exit(1)
	}

	fmt.Println("generated:", outPath)
}
