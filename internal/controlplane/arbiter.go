package controlplane

import (
	"crypto/sha256"
	"encoding/hex"
	"sort"
	"strconv"
	"strings"
	"time"
)

type ReasonCode string

const (
	ReasonSelected              ReasonCode = "selected"
	ReasonExpired               ReasonCode = "expired"
	ReasonInvalid               ReasonCode = "invalid"
	ReasonLowerAuthority        ReasonCode = "lower_authority"
	ReasonOlderSameAuthority    ReasonCode = "older_same_authority"
	ReasonDuplicate             ReasonCode = "duplicate"
	ReasonIdempotencyConflict   ReasonCode = "idempotency_conflict"
	ReasonDeterministicTieBreak ReasonCode = "deterministic_tie_break"
)

type CandidateOutcome struct {
	Intent     Intent
	ReasonCode ReasonCode
	Detail     string
}

type ArbitrationResult struct {
	ConflictKey ConflictKey
	Winner      *Intent
	Superseded  []CandidateOutcome
	Ignored     []CandidateOutcome
	ReasonCode  ReasonCode
}

type arbitrationBucket struct {
	key        ConflictKey
	candidates []Intent
	ignored    []CandidateOutcome
}

// Arbitrate is a pure function. It evaluates only candidate intent semantics;
// freeze, locks, freshness, permissions and rate limits belong to Safety Guard.
func Arbitrate(now time.Time, intents []Intent) []ArbitrationResult {
	working := make([]Intent, len(intents))
	for index := range intents {
		working[index] = cloneIntent(intents[index])
	}
	sort.Slice(working, func(left, right int) bool {
		return canonicalIntentKey(working[left]) < canonicalIntentKey(working[right])
	})

	buckets := make(map[ConflictKey]*arbitrationBucket)
	activeByIdempotency := make(map[string][]Intent)
	for _, intent := range working {
		bucket := ensureBucket(buckets, intent.ConflictKey())
		if err := intent.Validate(); err != nil {
			bucket.ignored = append(bucket.ignored, CandidateOutcome{Intent: intent, ReasonCode: ReasonInvalid, Detail: err.Error()})
			continue
		}
		if intent.Expired(now) {
			bucket.ignored = append(bucket.ignored, CandidateOutcome{Intent: intent, ReasonCode: ReasonExpired, Detail: "intent expiration is at or before arbitration time"})
			continue
		}
		activeByIdempotency[intent.IdempotencyKey] = append(activeByIdempotency[intent.IdempotencyKey], intent)
	}

	idempotencyKeys := make([]string, 0, len(activeByIdempotency))
	for key := range activeByIdempotency {
		idempotencyKeys = append(idempotencyKeys, key)
	}
	sort.Strings(idempotencyKeys)
	for _, key := range idempotencyKeys {
		candidates := activeByIdempotency[key]
		sort.Slice(candidates, func(left, right int) bool {
			if candidates[left].ID != candidates[right].ID {
				return candidates[left].ID < candidates[right].ID
			}
			return canonicalIntentKey(candidates[left]) < canonicalIntentKey(candidates[right])
		})
		if !sameIdempotentSemantics(candidates) {
			for _, intent := range candidates {
				bucket := ensureBucket(buckets, intent.ConflictKey())
				bucket.ignored = append(bucket.ignored, CandidateOutcome{Intent: intent, ReasonCode: ReasonIdempotencyConflict, Detail: "idempotency key is shared by different intent semantics"})
			}
			continue
		}
		bucket := ensureBucket(buckets, candidates[0].ConflictKey())
		bucket.candidates = append(bucket.candidates, candidates[0])
		for _, duplicate := range candidates[1:] {
			bucket.ignored = append(bucket.ignored, CandidateOutcome{Intent: duplicate, ReasonCode: ReasonDuplicate, Detail: "semantically identical idempotency key"})
		}
	}

	results := make([]ArbitrationResult, 0, len(buckets))
	for _, bucket := range buckets {
		sort.Slice(bucket.candidates, func(left, right int) bool {
			return candidateWins(bucket.candidates[left], bucket.candidates[right])
		})
		sortOutcomes(bucket.ignored)
		result := ArbitrationResult{ConflictKey: bucket.key, Ignored: append([]CandidateOutcome(nil), bucket.ignored...)}
		if len(bucket.candidates) > 0 {
			winner := cloneIntent(bucket.candidates[0])
			result.Winner = &winner
			result.ReasonCode = ReasonSelected
			for _, loser := range bucket.candidates[1:] {
				result.Superseded = append(result.Superseded, CandidateOutcome{
					Intent: cloneIntent(loser), ReasonCode: supersededReason(winner, loser), Detail: "candidate lost deterministic arbitration",
				})
			}
			sortOutcomes(result.Superseded)
		} else if len(result.Ignored) > 0 {
			result.ReasonCode = result.Ignored[0].ReasonCode
		}
		results = append(results, result)
	}
	sort.Slice(results, func(left, right int) bool {
		return results[left].ConflictKey.String() < results[right].ConflictKey.String()
	})
	return results
}

func ensureBucket(buckets map[ConflictKey]*arbitrationBucket, key ConflictKey) *arbitrationBucket {
	if bucket := buckets[key]; bucket != nil {
		return bucket
	}
	bucket := &arbitrationBucket{key: key}
	buckets[key] = bucket
	return bucket
}

func sameIdempotentSemantics(intents []Intent) bool {
	if len(intents) < 2 {
		return true
	}
	signature := semanticSignature(intents[0])
	for _, intent := range intents[1:] {
		if semanticSignature(intent) != signature {
			return false
		}
	}
	return true
}

// SemanticSignature returns a stable digest of every field that affects
// execution, arbitration, lifecycle, permission, or audit semantics.
func SemanticSignature(intent Intent) (string, error) {
	if err := intent.Validate(); err != nil {
		return "", err
	}
	digest := sha256.Sum256([]byte(semanticSignature(intent)))
	return "cp-sem-v1-" + hex.EncodeToString(digest[:]), nil
}

func candidateWins(left, right Intent) bool {
	leftRank, _ := authorityRank(left.Authority)
	rightRank, _ := authorityRank(right.Authority)
	if leftRank != rightRank {
		return leftRank < rightRank
	}
	if !left.CreatedAt.Equal(right.CreatedAt) {
		return left.CreatedAt.After(right.CreatedAt)
	}
	if left.ID != right.ID {
		return left.ID < right.ID
	}
	return canonicalIntentKey(left) < canonicalIntentKey(right)
}

func supersededReason(winner, loser Intent) ReasonCode {
	winnerRank, _ := authorityRank(winner.Authority)
	loserRank, _ := authorityRank(loser.Authority)
	if winnerRank != loserRank {
		return ReasonLowerAuthority
	}
	if !winner.CreatedAt.Equal(loser.CreatedAt) {
		return ReasonOlderSameAuthority
	}
	return ReasonDeterministicTieBreak
}

func sortOutcomes(outcomes []CandidateOutcome) {
	sort.Slice(outcomes, func(left, right int) bool {
		if outcomes[left].ReasonCode != outcomes[right].ReasonCode {
			return outcomes[left].ReasonCode < outcomes[right].ReasonCode
		}
		return canonicalIntentKey(outcomes[left].Intent) < canonicalIntentKey(outcomes[right].Intent)
	})
}

func semanticSignature(intent Intent) string {
	var builder strings.Builder
	appendCanonicalPart(&builder, intent.Producer.String())
	appendCanonicalPart(&builder, intent.Authority.String())
	appendCanonicalPart(&builder, intent.Resource.String())
	appendCanonicalPart(&builder, intent.Operation.String())
	appendCanonicalPart(&builder, intent.DesiredState.canonical())
	appendCanonicalPart(&builder, intent.Actor)
	appendCanonicalPart(&builder, intent.Reason)
	appendCanonicalPart(&builder, intent.PolicyVersion)
	appendCanonicalPart(&builder, intent.SnapshotVersion)
	appendCanonicalPart(&builder, strconv.FormatInt(intent.CreatedAt.UnixNano(), 10))
	if intent.ExpiresAt == nil {
		appendCanonicalPart(&builder, "no_expiration")
	} else {
		appendCanonicalPart(&builder, strconv.FormatInt(intent.ExpiresAt.UnixNano(), 10))
	}
	for _, reference := range canonicalEvidenceRefs(intent.EvidenceRefs) {
		appendCanonicalPart(&builder, reference)
	}
	return builder.String()
}

func canonicalIntentKey(intent Intent) string {
	var builder strings.Builder
	appendCanonicalPart(&builder, intent.ID)
	appendCanonicalPart(&builder, intent.IdempotencyKey)
	appendCanonicalPart(&builder, semanticSignature(intent))
	return builder.String()
}

func appendCanonicalPart(builder *strings.Builder, value string) {
	builder.WriteString(strconv.Itoa(len(value)))
	builder.WriteByte(':')
	builder.WriteString(value)
	builder.WriteByte('|')
}
