package gateway

import (
	"log/slog"
	"net/http"

	"opspilot/internal/auth"
)

// requiredRole decides which role a request needs, based on its HTTP method:
//
//	GET, HEAD        → viewer     (read)
//	POST, PUT, PATCH → responder  (create and change incidents, send alerts)
//	DELETE           → admin
//
// Any other method also needs admin, so something we didn't plan for is denied by default.
func requiredRole(method string) string {
	switch method {
	case http.MethodGet, http.MethodHead:
		return auth.RoleViewer
	case http.MethodPost, http.MethodPut, http.MethodPatch:
		return auth.RoleResponder
	default:
		return auth.RoleAdmin
	}
}

// RequireRole lets a request through only if the user's role allows it. It must run after
// RequireAuth, which puts the user's claims in the request context.
func RequireRole(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		claims, ok := ClaimsFrom(r.Context())
		if !ok {
			// Only happens if main.go wires the middleware in the wrong order.
			slog.Error("gateway: RequireRole used without RequireAuth", "path", r.URL.Path)
			writeError(w, http.StatusInternalServerError, "internal error")
			return
		}

		need := requiredRole(r.Method)
		if !auth.HasAtLeast(claims.Role, need) {
			slog.Warn("gateway: access denied",
				"user", claims.Subject, "role", claims.Role, "required_role", need,
				"method", r.Method, "path", r.URL.Path)
			writeError(w, http.StatusForbidden, "forbidden: this needs the "+need+" role")
			return
		}
		next.ServeHTTP(w, r)
	})
}
