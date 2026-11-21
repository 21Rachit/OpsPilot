package mcpserver

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"

	"opspilot/internal/model"
	"opspilot/internal/search"
)

// APIClient calls the OpsPilot HTTP API. The MCP server never touches the database itself:
// it is a thin adapter in front of the API, so every rule the API has still applies.
//
// With a username it logs in at the gateway and sends the token with every request, so the
// agent gets that user's role: logged in as a viewer, it can read but never change anything.
type APIClient struct {
	baseURL  string
	username string // empty: no login (talking to the server directly)
	password string
	http     *http.Client

	mu    sync.Mutex // guards token: the agent may call several tools at the same time
	token string
}

func NewAPIClient(baseURL, username, password string) *APIClient {
	return &APIClient{
		baseURL:  strings.TrimRight(baseURL, "/"),
		username: username,
		password: password,
		http:     &http.Client{Timeout: 10 * time.Second},
	}
}

// ListIncidents calls GET /incidents.
func (c *APIClient) ListIncidents(ctx context.Context) ([]model.Incident, error) {
	var incidents []model.Incident
	if err := c.get(ctx, "/incidents", &incidents); err != nil {
		return nil, err
	}
	return incidents, nil
}

// GetIncident calls GET /incidents/{id}.
func (c *APIClient) GetIncident(ctx context.Context, id int) (model.Incident, error) {
	var inc model.Incident
	err := c.get(ctx, "/incidents/"+strconv.Itoa(id), &inc)
	return inc, err
}

// Search calls GET /search. url.Values encodes the query, so spaces and & in it are safe.
func (c *APIClient) Search(ctx context.Context, query string, limit int, minScore float64) (search.Results, error) {
	q := url.Values{}
	q.Set("q", query)
	q.Set("limit", strconv.Itoa(limit))
	q.Set("min_score", strconv.FormatFloat(minScore, 'f', -1, 64))

	var res search.Results
	err := c.get(ctx, "/search?"+q.Encode(), &res)
	return res, err
}

// CreateIncident calls POST /incidents. Through the gateway this needs the responder role.
func (c *APIClient) CreateIncident(ctx context.Context, title, severity string) (model.Incident, error) {
	body, _ := json.Marshal(map[string]string{"title": title, "severity": severity})
	var inc model.Incident
	err := c.do(ctx, http.MethodPost, "/incidents", body, http.StatusCreated, &inc)
	return inc, err
}

// get sends a GET request and decodes the JSON answer into out.
func (c *APIClient) get(ctx context.Context, path string, out any) error {
	return c.do(ctx, http.MethodGet, path, nil, http.StatusOK, out)
}

// do sends one request (body may be nil), checks the status is want, and decodes the JSON
// answer into out.
func (c *APIClient) do(ctx context.Context, method, path string, body []byte, want int, out any) error {
	resp, err := c.send(ctx, method, path, body)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	// Tokens expire (TOKEN_TTL, 15 minutes by default), and the MCP server runs as long as
	// the agent's session. On 401, log in again once and retry.
	if resp.StatusCode == http.StatusUnauthorized && c.username != "" {
		resp.Body.Close()
		c.setToken("")
		if resp, err = c.send(ctx, method, path, body); err != nil {
			return err
		}
		defer resp.Body.Close()
	}

	if resp.StatusCode != want {
		return apiError(method, path, resp)
	}
	return json.NewDecoder(resp.Body).Decode(out)
}

// send makes one request, logging in first if we need a token and don't have one yet.
// body is a []byte, not an io.Reader, so a retry can send it again.
func (c *APIClient) send(ctx context.Context, method, path string, body []byte) (*http.Response, error) {
	req, err := http.NewRequestWithContext(ctx, method, c.baseURL+path, bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	if c.username != "" {
		token, err := c.currentToken(ctx)
		if err != nil {
			return nil, err
		}
		req.Header.Set("Authorization", "Bearer "+token)
	}
	resp, err := c.http.Do(req)
	if err != nil {
		return nil, fmt.Errorf("opspilot API unreachable at %s: %w", c.baseURL, err)
	}
	return resp, nil
}

// currentToken returns the saved token, or logs in to get one.
func (c *APIClient) currentToken(ctx context.Context) (string, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.token != "" {
		return c.token, nil
	}

	body, _ := json.Marshal(map[string]string{"username": c.username, "password": c.password})
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.baseURL+"/login", bytes.NewReader(body))
	if err != nil {
		return "", err
	}
	resp, err := c.http.Do(req)
	if err != nil {
		return "", fmt.Errorf("opspilot gateway unreachable at %s: %w", c.baseURL, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return "", apiError(http.MethodPost, "/login", resp)
	}

	var login struct {
		AccessToken string `json:"access_token"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&login); err != nil || login.AccessToken == "" {
		return "", errors.New("opspilot gateway: login returned no token")
	}
	slog.Info("mcp: logged in to gateway", "username", c.username) // never log the token
	c.token = login.AccessToken
	return c.token, nil
}

func (c *APIClient) setToken(t string) {
	c.mu.Lock()
	c.token = t
	c.mu.Unlock()
}

// apiError turns a non-2xx answer into an error. The API and the gateway answer errors as
// {"error": "..."}; that message is passed on so the agent can explain it.
func apiError(method, path string, resp *http.Response) error {
	var body struct {
		Error string `json:"error"`
	}
	_ = json.NewDecoder(io.LimitReader(resp.Body, 4096)).Decode(&body)
	return fmt.Errorf("opspilot API: %s %s returned %d %s", method, path, resp.StatusCode, body.Error)
}
