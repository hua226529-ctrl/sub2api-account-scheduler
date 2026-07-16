package sub2api

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/hua226529-ctrl/sub2api-account-scheduler/internal/model"
)

type Client struct {
	baseURL string
	apiKey  string
	http    *http.Client
}

type responseEnvelope[T any] struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
	Data    T      `json:"data"`
}

type paginated[T any] struct {
	Items    []T   `json:"items"`
	Total    int64 `json:"total"`
	Page     int   `json:"page"`
	PageSize int   `json:"page_size"`
	Pages    int   `json:"pages"`
}

func New(baseURL, apiKey string, timeout time.Duration) *Client {
	return &Client{
		baseURL: strings.TrimRight(baseURL, "/"),
		apiKey:  apiKey,
		http:    &http.Client{Timeout: timeout},
	}
}

func (c *Client) Validate(ctx context.Context, candidate string) error {
	var envelope responseEnvelope[paginated[model.Monitor]]
	return c.requestWithKey(ctx, http.MethodGet, "/api/v1/admin/channel-monitors?page=1&page_size=1", nil, &envelope, strings.TrimSpace(candidate))
}

func (c *Client) ListMonitors(ctx context.Context) ([]model.Monitor, error) {
	items := make([]model.Monitor, 0)
	for page := 1; ; page++ {
		path := "/api/v1/admin/channel-monitors?page=" + strconv.Itoa(page) + "&page_size=200"
		var envelope responseEnvelope[paginated[model.Monitor]]
		if err := c.get(ctx, path, &envelope); err != nil {
			return nil, err
		}
		items = append(items, envelope.Data.Items...)
		if page >= envelope.Data.Pages || len(envelope.Data.Items) == 0 {
			break
		}
	}
	return items, nil
}

func (c *Client) ListAccounts(ctx context.Context) ([]model.Account, error) {
	items := make([]model.Account, 0)
	for page := 1; ; page++ {
		path := "/api/v1/admin/accounts?page=" + strconv.Itoa(page) + "&page_size=200&sort_by=id&sort_order=asc"
		var envelope responseEnvelope[paginated[model.Account]]
		if err := c.get(ctx, path, &envelope); err != nil {
			return nil, err
		}
		items = append(items, envelope.Data.Items...)
		if page >= envelope.Data.Pages || len(envelope.Data.Items) == 0 {
			break
		}
	}
	return items, nil
}

func (c *Client) SetSchedulable(ctx context.Context, accountID int64, value bool) (model.Account, error) {
	body := map[string]bool{"schedulable": value}
	var envelope responseEnvelope[model.Account]
	path := fmt.Sprintf("/api/v1/admin/accounts/%d/schedulable", accountID)
	if err := c.request(ctx, http.MethodPost, path, body, &envelope); err != nil {
		return model.Account{}, err
	}
	return envelope.Data, nil
}

func (c *Client) UpdateLoadFactor(ctx context.Context, accountID int64, value *int) (model.Account, error) {
	target := 0
	if value != nil {
		target = *value
		if target < 1 {
			return model.Account{}, fmt.Errorf("load_factor must be positive or nil")
		}
	}
	body := map[string]int{"load_factor": target}
	var envelope responseEnvelope[model.Account]
	path := fmt.Sprintf("/api/v1/admin/accounts/%d", accountID)
	if err := c.request(ctx, http.MethodPut, path, body, &envelope); err != nil {
		return model.Account{}, err
	}
	if envelope.Data.ID != accountID {
		return model.Account{}, fmt.Errorf("sub2api returned account %d after updating account %d", envelope.Data.ID, accountID)
	}
	if value == nil {
		if envelope.Data.LoadFactor != nil && *envelope.Data.LoadFactor != 0 {
			return model.Account{}, fmt.Errorf("sub2api did not clear load_factor for account %d", accountID)
		}
		envelope.Data.LoadFactor = nil
	} else if envelope.Data.LoadFactor == nil || *envelope.Data.LoadFactor != target {
		return model.Account{}, fmt.Errorf("sub2api returned unexpected load_factor for account %d", accountID)
	}
	return envelope.Data, nil
}

func (c *Client) get(ctx context.Context, path string, out any) error {
	return c.request(ctx, http.MethodGet, path, nil, out)
}

func (c *Client) request(ctx context.Context, method, path string, body any, out any) error {
	return c.requestWithKey(ctx, method, path, body, out, c.apiKey)
}

func (c *Client) requestWithKey(ctx context.Context, method, path string, body any, out any, apiKey string) error {
	if _, err := url.ParseRequestURI(c.baseURL + path); err != nil {
		return err
	}
	var reader io.Reader
	if body != nil {
		payload, err := json.Marshal(body)
		if err != nil {
			return err
		}
		reader = bytes.NewReader(payload)
	}
	req, err := http.NewRequestWithContext(ctx, method, c.baseURL+path, reader)
	if err != nil {
		return err
	}
	req.Header.Set("x-api-key", apiKey)
	req.Header.Set("Accept", "application/json")
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := c.http.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	payload, err := io.ReadAll(io.LimitReader(resp.Body, 4<<20))
	if err != nil {
		return err
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("sub2api returned %d: %s", resp.StatusCode, strings.TrimSpace(string(payload)))
	}
	if err := json.Unmarshal(payload, out); err != nil {
		return fmt.Errorf("decode sub2api response: %w", err)
	}
	var status struct {
		Code    int    `json:"code"`
		Message string `json:"message"`
	}
	if err := json.Unmarshal(payload, &status); err == nil && status.Code != 0 {
		return fmt.Errorf("sub2api returned code %d: %s", status.Code, status.Message)
	}
	return nil
}
