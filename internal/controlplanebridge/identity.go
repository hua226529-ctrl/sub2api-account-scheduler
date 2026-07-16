package controlplanebridge

import (
	"crypto/sha256"
	"encoding/hex"
	"strconv"
	"strings"
	"time"
)

func normalizedContext(context LegacyContext) LegacyContext {
	context.StableSourceNamespace = StableSourceNamespace(strings.TrimSpace(string(context.StableSourceNamespace)))
	context.StableSourceID = strings.TrimSpace(context.StableSourceID)
	context.Actor = strings.TrimSpace(context.Actor)
	context.Reason = strings.TrimSpace(context.Reason)
	context.PolicyVersion = strings.TrimSpace(context.PolicyVersion)
	context.SnapshotVersion = strings.TrimSpace(context.SnapshotVersion)
	context.EvidenceRefs = append([]string(nil), context.EvidenceRefs...)
	for index := range context.EvidenceRefs {
		context.EvidenceRefs[index] = strings.TrimSpace(context.EvidenceRefs[index])
	}
	context.ExpiresAt = cloneTime(context.ExpiresAt)
	return context
}

func deriveDigest(prefix string, fields ...string) string {
	hash := sha256.New()
	for _, field := range fields {
		hash.Write([]byte(strconv.Itoa(len(field))))
		hash.Write([]byte{':'})
		hash.Write([]byte(field))
		hash.Write([]byte{'|'})
	}
	return prefix + hex.EncodeToString(hash.Sum(nil))
}

func hasEvidence(references []string) bool {
	if len(references) == 0 {
		return false
	}
	for _, reference := range references {
		if strings.TrimSpace(reference) == "" {
			return false
		}
	}
	return true
}

func cloneTime(value *time.Time) *time.Time {
	if value == nil {
		return nil
	}
	copy := *value
	return &copy
}
