package xstate

// Machine is a simplified, flattened view of an XState machine config.
type Machine struct {
	ID      string
	Initial string

	// States: key is flattened state name like "A.B.C"
	States map[string]*State

	// FinalStates: set of state names that are final.
	FinalStates map[string]bool
}

type State struct {
	Name  string
	On    map[string][]Transition // event -> transitions
	Entry []string                // XState "entry"
	Exit  []string                // XState "exit"
}

type Transition struct {
	Target  string
	Actions []string // XState transition "actions"
}
