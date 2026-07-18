package agent

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/hua226529-ctrl/sub2api-account-scheduler/internal/model"
)

type ModelDecision struct {
	Summary          string            `json:"summary"`
	Conclusion       string            `json:"conclusion"`
	Confidence       float64           `json:"confidence"`
	NoChange         bool              `json:"no_change"`
	Actions          []AgentAction     `json:"actions"`
	Advice           []string          `json:"advice"`
	DataLimitations  []string          `json:"data_limitations"`
	EvidenceRequests []EvidenceRequest `json:"evidence_requests"`
}

type EvidenceRequest struct {
	Tool      string `json:"tool"`
	AccountID int64  `json:"account_id,omitempty"`
	Pool      string `json:"pool,omitempty"`
	ScopeType string `json:"scope_type,omitempty"`
	ScopeID   string `json:"scope_id,omitempty"`
	RunID     int64  `json:"run_id,omitempty"`
	Limit     int    `json:"limit,omitempty"`
}

type ActionPrediction struct {
	SuccessRateDelta float64 `json:"success_rate_delta"`
	LatencyDeltaMS   int64   `json:"latency_delta_ms"`
	CostDelta        float64 `json:"cost_delta"`
}

type AgentAction struct {
	Type       string           `json:"type"`
	Arguments  json.RawMessage  `json:"arguments,omitempty"`
	AccountID  int64            `json:"account_id,omitempty"`
	SourceID   int64            `json:"source_id,omitempty"`
	KeyID      string           `json:"key_id,omitempty"`
	TargetTier string           `json:"target_tier,omitempty"`
	LoadFactor *int             `json:"load_factor,omitempty"`
	ScopeType  string           `json:"scope_type,omitempty"`
	ScopeID    string           `json:"scope_id,omitempty"`
	Config     json.RawMessage  `json:"config,omitempty"`
	PolicyID   int64            `json:"policy_id,omitempty"`
	Reason     string           `json:"reason"`
	Prediction ActionPrediction `json:"prediction"`
}

type completionClient struct{}

type chatRequest struct {
	Model          string        `json:"model"`
	Messages       []chatMessage `json:"messages"`
	Temperature    float64       `json:"temperature,omitempty"`
	MaxTokens      int           `json:"max_tokens,omitempty"`
	ResponseFormat any           `json:"response_format,omitempty"`
}

type chatMessage struct {
	Role    string `json:"role"`
	Content string `json:"content"`
}

type chatResponse struct {
	Choices []struct {
		Message struct {
			Content any `json:"content"`
		} `json:"message"`
	} `json:"choices"`
	Error *struct {
		Message string `json:"message"`
	} `json:"error,omitempty"`
}

func (completionClient) Complete(ctx context.Context, provider model.AgentProvider, apiKey, systemPrompt, userPrompt string) (ModelDecision, error) {
	systemPrompt = redactAgentPrompt(systemPrompt)
	userPrompt = redactAgentPrompt(userPrompt)
	request := chatRequest{
		Model: provider.Model, Temperature: provider.Temperature, MaxTokens: provider.MaxOutputTokens,
		Messages:       []chatMessage{{Role: "system", Content: systemPrompt}, {Role: "user", Content: userPrompt}},
		ResponseFormat: runtimeDecisionResponseFormat(true),
	}
	var decision ModelDecision
	payload, err := json.Marshal(request)
	if err != nil {
		return decision, err
	}
	response, status, err := doCompletion(ctx, provider, apiKey, payload)
	if err != nil {
		return decision, classifyProviderError(err)
	}
	if status == http.StatusBadRequest && responseFormatUnsupported(response) {
		request.ResponseFormat = runtimeDecisionResponseFormat(false)
		payload, _ = json.Marshal(request)
		response, status, err = doCompletion(ctx, provider, apiKey, payload)
		if err != nil {
			return decision, classifyProviderError(err)
		}
	}
	if status < 200 || status >= 300 {
		return decision, classifyProviderStatus(status, safeResponseMessage(response))
	}
	var envelope chatResponse
	if err := json.Unmarshal(response, &envelope); err != nil {
		return decision, errors.New("模型接口返回格式无效")
	}
	if envelope.Error != nil && envelope.Error.Message != "" {
		return decision, errors.New(envelope.Error.Message)
	}
	if len(envelope.Choices) == 0 {
		return decision, errors.New("模型没有返回分析结果")
	}
	return decodeRuntimeDecision(contentText(envelope.Choices[0].Message.Content))
}

func doCompletion(ctx context.Context, provider model.AgentProvider, apiKey string, payload []byte) ([]byte, int, error) {
	endpoint, err := completionEndpoint(provider.BaseURL)
	if err != nil {
		return nil, 0, &providerConfigurationError{err: err}
	}
	timeout := time.Duration(provider.TimeoutSeconds) * time.Second
	if timeout < 10*time.Second {
		timeout = 90 * time.Second
	}
	client := &http.Client{
		Timeout: timeout,
		CheckRedirect: func(req *http.Request, via []*http.Request) error {
			if len(via) > 0 && (req.URL.Scheme != via[0].URL.Scheme || !strings.EqualFold(req.URL.Host, via[0].URL.Host)) {
				return errors.New("模型接口禁止跨协议、主机或端口重定向")
			}
			if len(via) >= 3 {
				return errors.New("模型接口重定向次数过多")
			}
			return nil
		},
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewReader(payload))
	if err != nil {
		return nil, 0, err
	}
	request.Header.Set("Authorization", "Bearer "+apiKey)
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("Accept", "application/json")
	response, err := client.Do(request)
	if err != nil {
		return nil, 0, err
	}
	defer response.Body.Close()
	body, err := io.ReadAll(io.LimitReader(response.Body, 4<<20))
	return body, response.StatusCode, err
}

func completionEndpoint(base string) (string, error) {
	base = strings.TrimSpace(base)
	parsed, err := url.Parse(base)
	if err != nil || parsed.Scheme == "" || parsed.Host == "" {
		return "", errors.New("模型接口地址无效")
	}
	if parsed.Scheme != "https" && parsed.Scheme != "http" {
		return "", errors.New("模型接口只支持 HTTP 或 HTTPS")
	}
	if parsed.User != nil || parsed.RawQuery != "" || parsed.Fragment != "" {
		return "", errors.New("模型接口地址不能包含凭据、查询参数或片段")
	}
	hostname := strings.ToLower(parsed.Hostname())
	if hostname == "metadata.google.internal" || hostname == "metadata" {
		return "", errors.New("模型接口地址被安全策略禁止")
	}
	if ip := net.ParseIP(hostname); ip != nil {
		if ip.IsUnspecified() || ip.IsMulticast() || ip.IsLinkLocalUnicast() || ip.IsLinkLocalMulticast() {
			return "", errors.New("模型接口地址被安全策略禁止")
		}
	}
	if parsed.Scheme == "http" && hostname != "localhost" {
		ip := net.ParseIP(hostname)
		if ip == nil || !ip.IsLoopback() {
			return "", errors.New("非本机模型接口必须使用 HTTPS")
		}
	}
	clean := strings.TrimRight(base, "/")
	if strings.HasSuffix(clean, "/chat/completions") {
		return clean, nil
	}
	if strings.HasSuffix(clean, "/v1") {
		return clean + "/chat/completions", nil
	}
	return clean + "/v1/chat/completions", nil
}

func contentText(content any) string {
	switch value := content.(type) {
	case string:
		return value
	case []any:
		var result strings.Builder
		for _, part := range value {
			object, ok := part.(map[string]any)
			if !ok {
				continue
			}
			if text, ok := object["text"].(string); ok {
				result.WriteString(text)
			}
		}
		return result.String()
	default:
		payload, _ := json.Marshal(content)
		return string(payload)
	}
}

func safeResponseMessage(payload []byte) string {
	var envelope chatResponse
	if json.Unmarshal(payload, &envelope) == nil && envelope.Error != nil {
		return envelope.Error.Message
	}
	return "请求失败"
}
