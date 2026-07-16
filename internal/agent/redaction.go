package agent

import (
	"encoding/json"
	"regexp"
	"strings"
	"unicode"
)

var (
	agentLabeledSecret       = regexp.MustCompile(`(?i)(\b(?:api[_ -]?key|access[_ -]?token|refresh[_ -]?token|authorization|password|passwd|secret)\b|API\s*密钥|管理员密钥|登录密码|账户密码|账号密码|密码|密钥)\s*[:=：]\s*(?:"[^"]*"|'[^']*'|[^\s,，;；]+)`)
	agentJSONSecret          = regexp.MustCompile(`(?i)"(api[_ -]?key|access[_ -]?token|refresh[_ -]?token|authorization|password|passwd|secret)"\s*:\s*"[^"]*"`)
	agentEscapedJSONSecret   = regexp.MustCompile(`(?i)\\+"(api[_ -]?key|access[_ -]?token|refresh[_ -]?token|authorization|password|passwd|secret|api\s*密钥|管理员密钥|登录密码|账户密码|账号密码|密码|密钥)\\+"\s*:\s*\\+"[^"\\]*\\+"`)
	agentBearerSecret        = regexp.MustCompile(`(?i)\bbearer\s+[A-Za-z0-9._~+/=-]{12,}`)
	agentPrefixedKey         = regexp.MustCompile(`(?i)\b(?:sk|admin|sess|secret)-[A-Za-z0-9_.=-]{16,}\b`)
	agentPEMSecret           = regexp.MustCompile(`(?s)-----BEGIN [A-Z0-9 ]*PRIVATE KEY-----.*?-----END [A-Z0-9 ]*PRIVATE KEY-----`)
	agentURLUserinfo         = regexp.MustCompile(`(?i)\b([a-z][a-z0-9+.-]*://)[^/@\s]+@`)
	agentRawHexSecret        = regexp.MustCompile(`\b[[:xdigit:]]{64}\b`)
	agentLongBareToken       = regexp.MustCompile(`[A-Za-z0-9][A-Za-z0-9._~+/=-]{31,}`)
	agentStandaloneLine      = regexp.MustCompile(`(?m)^[\t ]*([^\r\n\t ]+)[\t ]*\r?$`)
	agentLikelyModelName     = regexp.MustCompile(`(?i)^(?:gpt|claude|gemini|qwen|deepseek|llama|o[134])[-_.]`)
	agentLikelyEmail         = regexp.MustCompile(`^[^@\s]+@[^@\s]+\.[^@\s]+$`)
	agentCommonBarePasswords = map[string]bool{
		"123456": true, "12345678": true, "123456789": true, "111111": true, "888888": true,
		"abc123": true, "admin123": true, "letmein": true, "password": true, "welcome123": true,
		"password123": true, "passw0rd": true, "qwerty": true, "qwerty123": true,
	}
)

// redactAgentText is applied before an administrator message is persisted or
// sent to a model. The agent has no secret-management capability, so retaining
// credentials in its 90-day conversation memory would provide no benefit.
func redactAgentText(value string) string {
	value = agentPEMSecret.ReplaceAllString(value, "[已脱敏私钥]")
	value = agentURLUserinfo.ReplaceAllString(value, "$1[已脱敏]@")
	value = agentBearerSecret.ReplaceAllString(value, "Bearer [已脱敏]")
	value = agentPrefixedKey.ReplaceAllString(value, "[已脱敏密钥]")
	value = agentRawHexSecret.ReplaceAllString(value, "[已脱敏密钥]")
	value = agentLongBareToken.ReplaceAllStringFunc(value, redactLongBareToken)
	value = agentLabeledSecret.ReplaceAllString(value, "$1：[已脱敏]")
	value = agentJSONSecret.ReplaceAllString(value, `"$1":"[已脱敏]"`)
	value = agentEscapedJSONSecret.ReplaceAllString(value, `\"[已脱敏字段]\":\"[已脱敏]\"`)
	value = agentStandaloneLine.ReplaceAllStringFunc(value, redactStandalonePasswordLine)
	return strings.TrimSpace(value)
}

func redactLongBareToken(value string) string {
	if looksLikeURLHostAndPath(value) {
		return value
	}
	return "[已脱敏令牌]"
}

func looksLikeURLHostAndPath(value string) bool {
	host := strings.SplitN(value, "/", 2)[0]
	labels := strings.Split(host, ".")
	if len(labels) < 2 {
		return false
	}
	tld := labels[len(labels)-1]
	if len(tld) < 2 || len(tld) > 24 {
		return false
	}
	for _, character := range tld {
		if !unicode.IsLetter(character) || character > unicode.MaxASCII {
			return false
		}
	}
	return true
}

func redactStandalonePasswordLine(line string) string {
	trimmed := strings.TrimSpace(line)
	if !looksLikeStandalonePassword(trimmed) {
		return line
	}
	return "[已脱敏密码]"
}

func looksLikeStandalonePassword(value string) bool {
	if value == "" || strings.Contains(value, "[已脱敏") || agentLikelyEmail.MatchString(value) || agentLikelyModelName.MatchString(value) {
		return false
	}
	lower := strings.ToLower(value)
	if agentCommonBarePasswords[lower] {
		return true
	}
	if len(value) < 7 || len(value) > 63 {
		return false
	}
	hasLetter, hasDigit, hasSymbol := false, false, false
	for _, character := range value {
		switch {
		case character > unicode.MaxASCII || unicode.IsSpace(character):
			return false
		case unicode.IsLetter(character):
			hasLetter = true
		case unicode.IsDigit(character):
			hasDigit = true
		default:
			if strings.ContainsRune(`/\\:,;`, character) {
				return false
			}
			hasSymbol = true
		}
	}
	if hasLetter && hasDigit {
		return true
	}
	return hasDigit && !hasLetter && !hasSymbol
}

// redactRuntimeMessages is the final model-boundary guard. It also protects
// historical rows created before the current ingestion redaction existed.
func redactRuntimeMessages(messages []RuntimeMessage) []RuntimeMessage {
	result := make([]RuntimeMessage, len(messages))
	for index, message := range messages {
		result[index] = message
		result[index].Content = redactAgentValue(message.Content)
		if len(message.ToolCalls) > 0 {
			result[index].ToolCalls = append([]RuntimeToolCall(nil), message.ToolCalls...)
			for callIndex := range result[index].ToolCalls {
				arguments := redactAgentJSONString(result[index].ToolCalls[callIndex].Function.Arguments)
				if json.Valid([]byte(arguments)) {
					result[index].ToolCalls[callIndex].Function.Arguments = arguments
				}
			}
		}
	}
	return result
}

func redactAgentValue(value any) any {
	switch typed := value.(type) {
	case nil:
		return nil
	case bool, float64, float32, int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64, json.Number:
		return typed
	case string:
		if json.Valid([]byte(typed)) {
			var decoded any
			if json.Unmarshal([]byte(typed), &decoded) == nil {
				switch decoded.(type) {
				case []any, map[string]any:
					if payload, err := json.Marshal(redactAgentValue(decoded)); err == nil {
						return string(payload)
					}
				}
			}
		}
		return redactAgentText(typed)
	case []any:
		items := make([]any, len(typed))
		for index := range typed {
			items[index] = redactAgentValue(typed[index])
		}
		return items
	case map[string]any:
		items := make(map[string]any, len(typed))
		for key, item := range typed {
			if isAgentSecretKey(key) {
				items[key] = "[已脱敏]"
			} else {
				items[key] = redactAgentValue(item)
			}
		}
		return items
	default:
		payload, err := json.Marshal(typed)
		if err != nil {
			return value
		}
		var decoded any
		if json.Unmarshal(payload, &decoded) != nil {
			return value
		}
		return redactAgentValue(decoded)
	}
}

func redactAgentJSONString(value string) string {
	var decoded any
	if json.Unmarshal([]byte(value), &decoded) != nil {
		return redactAgentText(value)
	}
	payload, err := json.Marshal(redactAgentValue(decoded))
	if err != nil {
		return redactAgentText(value)
	}
	return string(payload)
}

// redactAgentPrompt is the last guard for legacy, text-only completion APIs.
// Those APIs receive runtime messages as a JSON transcript inside a string, so
// treating the prompt as flat text would miss short secrets in escaped nested
// objects. Keep JSON prompts parseable while applying the same key-aware walk
// used by native tool messages.
func redactAgentPrompt(value string) string {
	redacted := redactAgentValue(value)
	if text, ok := redacted.(string); ok {
		return text
	}
	payload, err := json.Marshal(redacted)
	if err != nil {
		return redactAgentText(value)
	}
	return string(payload)
}

func isAgentSecretKey(key string) bool {
	key = strings.ToLower(strings.TrimSpace(key))
	key = strings.NewReplacer("-", "_", " ", "_").Replace(key)
	switch key {
	case "api_key", "apikey", "access_token", "refresh_token", "authorization", "password", "passwd", "secret",
		"管理员密钥", "登录密码", "账户密码", "账号密码", "密码", "密钥":
		return true
	default:
		return false
	}
}
