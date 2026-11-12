package gateway

import (
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httputil"
	"net/url"
)

// NewProxy returns a handler that forwards every request to upstream (the OpsPilot server)
// and copies the response back to the client.
func NewProxy(upstream string) (http.Handler, error) {
	target, err := url.Parse(upstream)
	if err != nil || target.Scheme == "" || target.Host == "" {
		return nil, fmt.Errorf("UPSTREAM_URL=%q must look like http://host:port", upstream)
	}

	return &httputil.ReverseProxy{
		// Rewrite builds the outgoing request from the incoming one.
		Rewrite: func(pr *httputil.ProxyRequest) {
			pr.SetURL(target)  // send it to the upstream, keeping the path and query
			pr.SetXForwarded() // tell the upstream who the real client is (X-Forwarded-For)
		},
		// ErrorHandler runs when the upstream can't be reached.
		ErrorHandler: func(w http.ResponseWriter, r *http.Request, err error) {
			slog.Error("gateway: upstream unreachable", "upstream", target.Host, "path", r.URL.Path, "error", err)
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusBadGateway)
			w.Write([]byte(`{"error":"upstream unavailable"}` + "\n"))
		},
	}, nil
}
