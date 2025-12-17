package util

import "strings"

func ExpandNode(s, nodeID string) string {
    return strings.ReplaceAll(s, "%NODE%", nodeID)
}
