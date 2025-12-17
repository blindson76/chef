package xstate

import (
	"encoding/json"
	"fmt"
)

func ParseMachineJSON(b []byte) (*Machine, error) {
	// Accept either:
	// 1) pure machine config object
	// 2) wrapper { "machine": { ... } }
	var raw any
	if err := json.Unmarshal(b, &raw); err != nil {
		return nil, err
	}

	obj, ok := raw.(map[string]any)
	if !ok {
		return nil, fmt.Errorf("expected JSON object at top-level")
	}

	if m2, ok := obj["machine"].(map[string]any); ok {
		obj = m2
	}

	m := &Machine{
		States:      map[string]*State{},
		FinalStates: map[string]bool{},
	}

	if id, _ := obj["id"].(string); id != "" {
		m.ID = id
	}
	if initial, _ := obj["initial"].(string); initial != "" {
		m.Initial = initial
	}

	statesObj, _ := obj["states"].(map[string]any)
	if statesObj == nil {
		return nil, fmt.Errorf(`missing "states" object`)
	}

	flattenStates(m, "", statesObj)

	if m.Initial == "" {
		for k := range statesObj {
			m.Initial = k
			break
		}
	}
	if m.Initial == "" {
		return nil, fmt.Errorf("could not determine initial state")
	}

	if _, ok := m.States[m.Initial]; !ok {
		return nil, fmt.Errorf("initial state %q not found in parsed states", m.Initial)
	}

	return m, nil
}

func flattenStates(m *Machine, prefix string, states map[string]any) {
	for stateName, v := range states {
		full := stateName
		if prefix != "" {
			full = prefix + "." + stateName
		}

		state := &State{
			Name: full,
			On:   map[string][]Transition{},
		}

		cfg, _ := v.(map[string]any)
		if cfg == nil {
			m.States[full] = state
			continue
		}

		if t, _ := cfg["type"].(string); t == "final" {
			m.FinalStates[full] = true
		}

		if ev, ok := cfg["entry"]; ok {
			state.Entry = parseActionList(ev)
		}
		if ev, ok := cfg["exit"]; ok {
			state.Exit = parseActionList(ev)
		}

		if onObj, _ := cfg["on"].(map[string]any); onObj != nil {
			for evt, tv := range onObj {
				state.On[evt] = parseTransitions(tv)
			}
		}

		m.States[full] = state

		if nested, _ := cfg["states"].(map[string]any); nested != nil {
			flattenStates(m, full, nested)
		}
	}
}

func parseTransitions(v any) []Transition {
	out := []Transition{}
	switch t := v.(type) {
	case string:
		out = append(out, Transition{Target: t})
	case map[string]any:
		tr := Transition{}
		if target, _ := t["target"].(string); target != "" {
			tr.Target = target
		}
		if av, ok := t["actions"]; ok {
			tr.Actions = parseActionList(av)
		}
		if tr.Target != "" {
			out = append(out, tr)
		}
	case []any:
		for _, item := range t {
			out = append(out, parseTransitions(item)...)
		}
	}
	return out
}

func parseActionList(v any) []string {
	var out []string
	switch t := v.(type) {
	case string:
		if t != "" {
			out = append(out, t)
		}
	case []any:
		for _, it := range t {
			out = append(out, parseActionList(it)...)
		}
	case map[string]any:
		if typ, _ := t["type"].(string); typ != "" {
			out = append(out, typ)
		}
	}
	return out
}
