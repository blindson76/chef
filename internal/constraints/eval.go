package constraints

import (
    "regexp"
    "strings"

    "github.com/umitbozkurt/orchestrator/internal/config"
)

type Result struct {
    OK     bool
    Reason string
}

func Evaluate(tags map[string]string, clauses []config.ConstraintClause) Result {
    for _, c := range clauses {
        if r := evalOne(tags, c); !r.OK {
            return r
        }
    }
    return Result{OK: true}
}

func evalOne(tags map[string]string, c config.ConstraintClause) Result {
    key := strings.TrimSpace(c.Key)
    op := strings.ToLower(strings.TrimSpace(c.Op))
    v, has := tags[key]

    switch op {
    case "exists":
        if !has {
            return Result{OK: false, Reason: "missing tag " + key}
        }
        return Result{OK: true}
    case "not_exists":
        if has {
            return Result{OK: false, Reason: "tag exists " + key}
        }
        return Result{OK: true}
    case "equals":
        if !has || v != c.Value {
            return Result{OK: false, Reason: key + " != " + c.Value}
        }
        return Result{OK: true}
    case "not_equals":
        if has && v == c.Value {
            return Result{OK: false, Reason: key + " == " + c.Value}
        }
        return Result{OK: true}
    case "in":
        if !has {
            return Result{OK: false, Reason: "missing tag " + key}
        }
        for _, vv := range c.Values {
            if v == vv {
                return Result{OK: true}
            }
        }
        return Result{OK: false, Reason: key + " not in list"}
    case "not_in":
        if !has {
            return Result{OK: true}
        }
        for _, vv := range c.Values {
            if v == vv {
                return Result{OK: false, Reason: key + " in blocked list"}
            }
        }
        return Result{OK: true}
    case "matches":
        if !has {
            return Result{OK: false, Reason: "missing tag " + key}
        }
        re, err := regexp.Compile(c.Value)
        if err != nil {
            return Result{OK: false, Reason: "bad regex"}
        }
        if !re.MatchString(v) {
            return Result{OK: false, Reason: key + " regex mismatch"}
        }
        return Result{OK: true}
    default:
        return Result{OK: false, Reason: "unknown op " + c.Op}
    }
}
