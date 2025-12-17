package gen

import (
	"bytes"
	"crypto/sha1"
	"fmt"
	"go/format"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"text/template"

	"github.com/umitbozkurt/orchestrator/internal/xstate"
)

type Options struct {
	PackageName string
	TypeName    string
	Machine     *xstate.Machine
	SourcePath  string
}

type tmplState struct {
	Name  string
	Ident string
}

type tmplTrigger struct {
	Name  string
	Ident string
}

type tmplTransition struct {
	FromIdent    string
	ToIdent      string
	TriggerIdent string
}

type tmplTransActions struct {
	FromIdent    string
	TriggerIdent string
	ToIdent      string
	Methods      []string
}

type tmplData struct {
	Package      string
	TypeName     string
	SourcePath   string
	InitialIdent string

	States      []tmplState
	Triggers    []tmplTrigger
	Transitions []tmplTransition

	ActionMethods []string
	EntryActions  map[string][]string // stateIdent -> methods
	ExitActions   map[string][]string // stateIdent -> methods
	TransActions  []tmplTransActions
}

func Generate(opt Options) ([]byte, error) {
	if opt.Machine == nil {
		return nil, fmt.Errorf("Machine is nil")
	}
	pkg := opt.PackageName
	if pkg == "" {
		pkg = "machine"
	}
	if opt.TypeName == "" {
		opt.TypeName = "Machine"
	}

	// Collect and sort states
	stateNames := make([]string, 0, len(opt.Machine.States))
	for name := range opt.Machine.States {
		stateNames = append(stateNames, name)
	}
	sort.Strings(stateNames)

	states := make([]tmplState, 0, len(stateNames))
	stateIdent := map[string]string{}
	for _, name := range stateNames {
		id := identFromState(name)
		id = makeUniqueIdent(id, stateIdent, name)
		stateIdent[name] = id
		states = append(states, tmplState{Name: name, Ident: id})
	}

	// Collect triggers and transitions
	triggerSet := map[string]bool{}
	var transitions []tmplTransition

	for _, from := range stateNames {
		s := opt.Machine.States[from]
		events := make([]string, 0, len(s.On))
		for evt := range s.On {
			events = append(events, evt)
		}
		sort.Strings(events)

		for _, evt := range events {
			triggerSet[evt] = true
			trs := s.On[evt]

			// stable per event
			var targets []string
			for _, tr := range trs {
				if tr.Target != "" {
					targets = append(targets, tr.Target)
				}
			}
			sort.Strings(targets)

			for _, to := range targets {
				if _, ok := stateIdent[to]; !ok {
					continue
				}
				transitions = append(transitions, tmplTransition{
					FromIdent:    stateIdent[from],
					ToIdent:      stateIdent[to],
					TriggerIdent: identFromTrigger(evt),
				})
			}
		}
	}

	sort.Slice(transitions, func(i, j int) bool {
		a, b := transitions[i], transitions[j]
		if a.FromIdent != b.FromIdent {
			return a.FromIdent < b.FromIdent
		}
		if a.TriggerIdent != b.TriggerIdent {
			return a.TriggerIdent < b.TriggerIdent
		}
		return a.ToIdent < b.ToIdent
	})

	// Triggers
	triggerNames := make([]string, 0, len(triggerSet))
	for t := range triggerSet {
		triggerNames = append(triggerNames, t)
	}
	sort.Strings(triggerNames)
	triggers := make([]tmplTrigger, 0, len(triggerNames))
	for _, t := range triggerNames {
		triggers = append(triggers, tmplTrigger{Name: t, Ident: identFromTrigger(t)})
	}

	initialIdent, ok := stateIdent[opt.Machine.Initial]
	if !ok {
		return nil, fmt.Errorf("initial state %q not found in generated states", opt.Machine.Initial)
	}

	// Actions: collect unique methods + per-state entry/exit + per-transition action sets
	actionSet := map[string]bool{}
	entryActions := map[string][]string{}
	exitActions := map[string][]string{}
	var transActions []tmplTransActions

	actionMethod := func(x string) string { return exportedName(x) }

	for _, stName := range stateNames {
		st := opt.Machine.States[stName]
		stID := stateIdent[stName]

		for _, a := range st.Entry {
			mn := actionMethod(a)
			actionSet[mn] = true
			entryActions[stID] = append(entryActions[stID], mn)
		}
		for _, a := range st.Exit {
			mn := actionMethod(a)
			actionSet[mn] = true
			exitActions[stID] = append(exitActions[stID], mn)
		}
	}

	for _, from := range stateNames {
		s := opt.Machine.States[from]
		events := make([]string, 0, len(s.On))
		for evt := range s.On {
			events = append(events, evt)
		}
		sort.Strings(events)

		for _, evt := range events {
			trs := s.On[evt]
			for _, tr := range trs {
				if tr.Target == "" {
					continue
				}
				if _, ok := stateIdent[tr.Target]; !ok {
					continue
				}

				methods := make([]string, 0, len(tr.Actions))
				for _, a := range tr.Actions {
					mn := actionMethod(a)
					actionSet[mn] = true
					methods = append(methods, mn)
				}
				sort.Strings(methods)

				if len(methods) > 0 {
					transActions = append(transActions, tmplTransActions{
						FromIdent:    stateIdent[from],
						TriggerIdent: identFromTrigger(evt),
						ToIdent:      stateIdent[tr.Target],
						Methods:      methods,
					})
				}
			}
		}
	}

	for k := range entryActions {
		sort.Strings(entryActions[k])
	}
	for k := range exitActions {
		sort.Strings(exitActions[k])
	}
	sort.Slice(transActions, func(i, j int) bool {
		a, b := transActions[i], transActions[j]
		if a.FromIdent != b.FromIdent {
			return a.FromIdent < b.FromIdent
		}
		if a.TriggerIdent != b.TriggerIdent {
			return a.TriggerIdent < b.TriggerIdent
		}
		return a.ToIdent < b.ToIdent
	})

	actionMethods := make([]string, 0, len(actionSet))
	for mn := range actionSet {
		actionMethods = append(actionMethods, mn)
	}
	sort.Strings(actionMethods)

	td := tmplData{
		Package:       safePackage(pkg),
		TypeName:      exportedName(opt.TypeName),
		SourcePath:    opt.SourcePath,
		InitialIdent:  initialIdent,
		States:        states,
		Triggers:      triggers,
		Transitions:   transitions,
		ActionMethods: actionMethods,
		EntryActions:  entryActions,
		ExitActions:   exitActions,
		TransActions:  transActions,
	}

	tpl, err := template.New("file").Parse(fileTemplate)
	if err != nil {
		return nil, err
	}

	var buf bytes.Buffer
	if err := tpl.Execute(&buf, td); err != nil {
		return nil, err
	}

	formatted, err := format.Source(buf.Bytes())
	if err != nil {
		return buf.Bytes(), fmt.Errorf("gofmt: %w", err)
	}
	return formatted, nil
}

func DefaultTypeName(machineID, inPath string) string {
	if machineID != "" {
		return exportedName(machineID)
	}
	base := filepath.Base(inPath)
	base = strings.TrimSuffix(base, filepath.Ext(base))
	return exportedName(base)
}

func safePackage(s string) string {
	s = strings.TrimSpace(s)
	if s == "" {
		return "machine"
	}
	s = strings.ToLower(s)
	re := regexp.MustCompile(`[^a-z0-9_]+`)
	s = re.ReplaceAllString(s, "_")
	if len(s) > 0 && s[0] >= '0' && s[0] <= '9' {
		s = "pkg_" + s
	}
	return s
}

func exportedName(s string) string {
	s = strings.TrimSpace(s)
	if s == "" {
		return "Machine"
	}
	re := regexp.MustCompile(`[^a-zA-Z0-9]+`)
	parts := re.Split(s, -1)
	var b strings.Builder
	for _, p := range parts {
		if p == "" {
			continue
		}
		b.WriteString(strings.ToUpper(p[:1]))
		if len(p) > 1 {
			b.WriteString(p[1:])
		}
	}
	out := b.String()
	if out == "" {
		return "Machine"
	}
	if out[0] >= '0' && out[0] <= '9' {
		out = "M" + out
	}
	return out
}

func identFromState(name string) string {
	re := regexp.MustCompile(`[^a-zA-Z0-9]+`)
	s := re.ReplaceAllString(name, "_")
	return exportedName(s)
}

func identFromTrigger(name string) string {
	re := regexp.MustCompile(`[^a-zA-Z0-9]+`)
	s := re.ReplaceAllString(name, "_")
	return exportedName(s)
}

func makeUniqueIdent(ident string, existing map[string]string, stateName string) string {
	used := map[string]bool{}
	for _, v := range existing {
		used[v] = true
	}
	if !used[ident] {
		return ident
	}
	h := sha1.Sum([]byte(stateName))
	suf := fmt.Sprintf("%x", h[:3])
	cand := ident + "_" + strings.ToUpper(suf)
	if !used[cand] {
		return cand
	}
	i := 2
	for {
		cand2 := fmt.Sprintf("%s_%d", cand, i)
		if !used[cand2] {
			return cand2
		}
		i++
	}
}
