package aionbd

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
)

type Error struct {
	Status int
	Method string
	Path   string
	Body   string
	Err    error
}

func (e *Error) Error() string {
	if e.Status > 0 {
		return fmt.Sprintf("HTTP %d on %s %s: %s", e.Status, e.Method, e.Path, e.Body)
	}
	if e.Err != nil {
		return fmt.Sprintf("request failed for %s %s: %v", e.Method, e.Path, e.Err)
	}
	return fmt.Sprintf("request failed for %s %s", e.Method, e.Path)
}

func (e *Error) Unwrap() error {
	return e.Err
}

type Client struct {
	baseURL       string
	httpClient    *http.Client
	apiKey        string
	bearerToken   string
	defaultHeader map[string]string
}

func NewClient(baseURL string, options *ClientOptions) *Client {
	if strings.TrimSpace(baseURL) == "" {
		baseURL = DefaultBaseURL
	}
	baseURL = strings.TrimRight(baseURL, "/")

	opts := ClientOptions{}
	if options != nil {
		opts = *options
	}

	httpClient := opts.HTTPClient
	if httpClient == nil {
		timeout := opts.Timeout
		if timeout <= 0 {
			timeout = DefaultTimeout
		}
		httpClient = &http.Client{Timeout: timeout}
	}

	headers := make(map[string]string, len(opts.Headers))
	for key, value := range opts.Headers {
		headers[key] = value
	}

	return &Client{
		baseURL:       baseURL,
		httpClient:    httpClient,
		apiKey:        opts.APIKey,
		bearerToken:   opts.BearerToken,
		defaultHeader: headers,
	}
}

func (c *Client) Live(ctx context.Context) (LiveResponse, error) {
	var response LiveResponse
	err := c.requestJSON(ctx, http.MethodGet, "/live", nil, &response)
	return response, err
}

func (c *Client) Ready(ctx context.Context) (ReadyResponse, error) {
	var response ReadyResponse
	err := c.requestJSON(ctx, http.MethodGet, "/ready", nil, &response)
	return response, err
}

func (c *Client) Health(ctx context.Context) (ReadyResponse, error) {
	return c.Ready(ctx)
}

func (c *Client) Metrics(ctx context.Context) (MetricsResponse, error) {
	var response MetricsResponse
	err := c.requestJSON(ctx, http.MethodGet, "/metrics", nil, &response)
	return response, err
}

func (c *Client) MetricsPrometheus(ctx context.Context) (string, error) {
	return c.requestRaw(ctx, http.MethodGet, "/metrics/prometheus", nil)
}

func (c *Client) Distance(ctx context.Context, left []float32, right []float32, metric Metric) (DistanceResponse, error) {
	body := map[string]any{
		"left":   left,
		"right":  right,
		"metric": withMetricDefault(metric),
	}
	var response DistanceResponse
	err := c.requestJSON(ctx, http.MethodPost, "/distance", body, &response)
	return response, err
}

func (c *Client) CreateCollection(ctx context.Context, name string, dimension int, strictFinite bool) (CollectionResponse, error) {
	body := map[string]any{
		"name":          name,
		"dimension":     dimension,
		"strict_finite": strictFinite,
	}
	var response CollectionResponse
	err := c.requestJSON(ctx, http.MethodPost, "/collections", body, &response)
	return response, err
}

func (c *Client) ListCollections(ctx context.Context) (ListCollectionsResponse, error) {
	var response ListCollectionsResponse
	err := c.requestJSON(ctx, http.MethodGet, "/collections", nil, &response)
	return response, err
}

func (c *Client) GetCollection(ctx context.Context, name string) (CollectionResponse, error) {
	path := fmt.Sprintf("/collections/%s", url.PathEscape(strings.TrimSpace(name)))
	var response CollectionResponse
	err := c.requestJSON(ctx, http.MethodGet, path, nil, &response)
	return response, err
}

func (c *Client) SearchCollection(ctx context.Context, collection string, query []float32, options *SearchOptions) (SearchResponse, error) {
	body := c.searchBody(query, options)
	path := fmt.Sprintf("/collections/%s/search", url.PathEscape(strings.TrimSpace(collection)))
	var response SearchResponse
	err := c.requestJSON(ctx, http.MethodPost, path, body, &response)
	return response, err
}

func (c *Client) SearchCollectionTopK(ctx context.Context, collection string, query []float32, options *SearchTopKOptions) (SearchTopKResponse, error) {
	body, err := c.searchTopKBody(query, options)
	if err != nil {
		return SearchTopKResponse{}, err
	}
	path := fmt.Sprintf("/collections/%s/search/topk", url.PathEscape(strings.TrimSpace(collection)))
	var response SearchTopKResponse
	err = c.requestJSON(ctx, http.MethodPost, path, body, &response)
	return response, err
}

func (c *Client) SearchCollectionTopKBatch(ctx context.Context, collection string, queries [][]float32, options *SearchTopKOptions) (SearchTopKBatchResponse, error) {
	body, err := c.searchTopKBody(nil, options)
	if err != nil {
		return SearchTopKBatchResponse{}, err
	}
	body["queries"] = queries
	delete(body, "query")
	path := fmt.Sprintf("/collections/%s/search/topk/batch", url.PathEscape(strings.TrimSpace(collection)))
	var response SearchTopKBatchResponse
	err = c.requestJSON(ctx, http.MethodPost, path, body, &response)
	return response, err
}

func (c *Client) UpsertPoint(ctx context.Context, collection string, pointID uint64, values []float32, payload PointPayload) (UpsertPointResponse, error) {
	body := map[string]any{"values": values}
	if payload != nil {
		body["payload"] = payload
	}
	path := fmt.Sprintf("/collections/%s/points/%d", url.PathEscape(strings.TrimSpace(collection)), pointID)
	var response UpsertPointResponse
	err := c.requestJSON(ctx, http.MethodPut, path, body, &response)
	return response, err
}

func (c *Client) UpsertPointsBatch(ctx context.Context, collection string, points []UpsertPointsBatchItem) (UpsertPointsBatchResponse, error) {
	body := map[string]any{"points": points}
	path := fmt.Sprintf("/collections/%s/points", url.PathEscape(strings.TrimSpace(collection)))
	var response UpsertPointsBatchResponse
	err := c.requestJSON(ctx, http.MethodPost, path, body, &response)
	return response, err
}

func (c *Client) GetPoint(ctx context.Context, collection string, pointID uint64) (PointResponse, error) {
	path := fmt.Sprintf("/collections/%s/points/%d", url.PathEscape(strings.TrimSpace(collection)), pointID)
	var response PointResponse
	err := c.requestJSON(ctx, http.MethodGet, path, nil, &response)
	return response, err
}

func (c *Client) ListPoints(ctx context.Context, collection string, options *ListPointsOptions) (ListPointsResponse, error) {
	offset := 0
	limit := 100
	includeLimit := true
	var afterID *uint64
	if options != nil {
		offset = options.Offset
		afterID = options.AfterID
		if options.Limit == nil {
			includeLimit = false
		} else {
			limit = *options.Limit
		}
	}

	if offset < 0 {
		return ListPointsResponse{}, fmt.Errorf("offset must be a non-negative integer")
	}
	if afterID != nil && offset != 0 {
		return ListPointsResponse{}, fmt.Errorf("offset must be 0 when afterID is provided")
	}
	if includeLimit && limit <= 0 {
		return ListPointsResponse{}, fmt.Errorf("limit must be a positive integer")
	}

	params := url.Values{}
	if includeLimit {
		params.Set("limit", strconv.Itoa(limit))
	}
	if afterID != nil {
		params.Set("after_id", strconv.FormatUint(*afterID, 10))
	} else {
		params.Set("offset", strconv.Itoa(offset))
	}
	path := fmt.Sprintf("/collections/%s/points?%s", url.PathEscape(strings.TrimSpace(collection)), params.Encode())
	var response ListPointsResponse
	err := c.requestJSON(ctx, http.MethodGet, path, nil, &response)
	return response, err
}

func (c *Client) DeletePoint(ctx context.Context, collection string, pointID uint64) (DeletePointResponse, error) {
	path := fmt.Sprintf("/collections/%s/points/%d", url.PathEscape(strings.TrimSpace(collection)), pointID)
	var response DeletePointResponse
	err := c.requestJSON(ctx, http.MethodDelete, path, nil, &response)
	return response, err
}

func (c *Client) DeleteCollection(ctx context.Context, name string) (DeleteCollectionResponse, error) {
	path := fmt.Sprintf("/collections/%s", url.PathEscape(strings.TrimSpace(name)))
	var response DeleteCollectionResponse
	err := c.requestJSON(ctx, http.MethodDelete, path, nil, &response)
	return response, err
}

func (c *Client) searchBody(query []float32, options *SearchOptions) map[string]any {
	metric := MetricDot
	mode := SearchModeAuto
	body := map[string]any{"query": query}
	if options != nil {
		metric = withMetricDefault(options.Metric)
		mode = withModeDefault(options.Mode)
		if options.TargetRecall != nil {
			body["target_recall"] = *options.TargetRecall
		}
		if options.Filter != nil {
			body["filter"] = options.Filter
		}
		if options.IncludePayload != nil {
			body["include_payload"] = *options.IncludePayload
		}
	} else {
		metric = MetricDot
		mode = SearchModeAuto
	}
	body["metric"] = metric
	body["mode"] = mode
	return body
}

func (c *Client) searchTopKBody(query []float32, options *SearchTopKOptions) (map[string]any, error) {
	searchOptions := (*SearchOptions)(nil)
	if options != nil {
		searchOptions = &options.SearchOptions
	}
	body := c.searchBody(query, searchOptions)
	limit := 10
	limitSet := options == nil
	if options != nil && options.Limit != nil {
		limit = *options.Limit
		limitSet = true
	}
	if limitSet {
		if limit <= 0 {
			return nil, fmt.Errorf("limit must be a positive integer")
		}
		body["limit"] = limit
	}
	return body, nil
}

func withMetricDefault(metric Metric) Metric {
	if metric == "" {
		return MetricDot
	}
	return metric
}

func withModeDefault(mode SearchMode) SearchMode {
	if mode == "" {
		return SearchModeAuto
	}
	return mode
}

func (c *Client) requestJSON(ctx context.Context, method string, path string, body any, out any) error {
	payload, err := c.doRequest(ctx, method, path, body, false)
	if err != nil {
		return err
	}
	if len(bytes.TrimSpace(payload)) == 0 {
		return nil
	}
	if err := json.Unmarshal(payload, out); err != nil {
		return &Error{
			Method: method,
			Path:   path,
			Body:   string(payload),
			Err:    fmt.Errorf("invalid JSON response: %w", err),
		}
	}
	return nil
}

func (c *Client) requestRaw(ctx context.Context, method string, path string, body any) (string, error) {
	payload, err := c.doRequest(ctx, method, path, body, true)
	if err != nil {
		return "", err
	}
	return string(payload), nil
}

func (c *Client) doRequest(ctx context.Context, method string, path string, body any, raw bool) ([]byte, error) {
	if ctx == nil {
		ctx = context.Background()
	}

	var requestBody io.Reader
	if body != nil {
		encoded, err := json.Marshal(body)
		if err != nil {
			return nil, &Error{Method: method, Path: path, Err: err}
		}
		requestBody = bytes.NewReader(encoded)
	}

	request, err := http.NewRequestWithContext(ctx, method, c.baseURL+path, requestBody)
	if err != nil {
		return nil, &Error{Method: method, Path: path, Err: err}
	}
	if raw {
		request.Header.Set("Accept", "text/plain")
	} else {
		request.Header.Set("Accept", "application/json")
	}
	for key, value := range c.defaultHeader {
		request.Header.Set(key, value)
	}
	if c.apiKey != "" {
		request.Header.Set("x-api-key", c.apiKey)
	}
	if c.bearerToken != "" {
		request.Header.Set("Authorization", "Bearer "+c.bearerToken)
	}
	if body != nil {
		request.Header.Set("Content-Type", "application/json")
	}

	response, err := c.httpClient.Do(request)
	if err != nil {
		return nil, &Error{Method: method, Path: path, Err: err}
	}
	defer response.Body.Close()

	responseBody, err := io.ReadAll(response.Body)
	if err != nil {
		return nil, &Error{Method: method, Path: path, Err: err}
	}
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		return nil, &Error{
			Status: response.StatusCode,
			Method: method,
			Path:   path,
			Body:   string(responseBody),
		}
	}
	return responseBody, nil
}
