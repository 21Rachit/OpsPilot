package gateway

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"strings"
	"time"

	"github.com/golang-jwt/jwt/v5"

	"opspilot/internal/auth"
)

// LoginHandler checks a username and password and returns a token.
//
//	POST /login {"username":"bob","password":"password"}
//	→ 200 {"access_token":"eyJ...","token_type":"Bearer","expires_in":900}
func LoginHandler(users *auth.UserStore, tokens *auth.TokenService) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			Username string `json:"username"`
			Password string `json:"password"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.Username == "" || req.Password == "" {
			writeError(w, http.StatusBadRequest, "username and password are required")
			return
		}

		user, ok := users.Authenticate(req.Username, req.Password)
		if !ok {
			// Same message for a wrong username and a wrong password, so it doesn't
			// reveal which usernames exist.
			slog.Warn("gateway: login failed", "username", req.Username)
			writeError(w, http.StatusUnauthorized, "invalid username or password")
			return
		}

		token, expires, err := tokens.Issue(user)
		if err != nil {
			slog.Error("gateway: could not issue token", "error", err)
			writeError(w, http.StatusInternalServerError, "internal error")
			return
		}
		slog.Info("gateway: login succeeded", "username", user.Username, "role", user.Role)
		writeJSON(w, http.StatusOK, map[string]any{
			"access_token": token,
			"token_type":   "Bearer",
			"expires_in":   int(time.Until(expires).Round(time.Second).Seconds()), // seconds, as the OAuth 2.0 spec uses
		})
	}
}

// RequireAuth lets a request through only if it has a valid token:
//
//	Authorization: Bearer <token>
//
// It then tells the upstream who the user is with the X-User and X-User-Role headers.
func RequireAuth(tokens *auth.TokenService, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Remove these first: a client must never be able to send them and pretend
		// to be someone else. Only the gateway sets them.
		r.Header.Del("X-User")
		r.Header.Del("X-User-Role")

		tokenString, found := strings.CutPrefix(r.Header.Get("Authorization"), "Bearer ")
		if !found || tokenString == "" {
			writeUnauthorized(w, "missing bearer token")
			return
		}

		claims, err := tokens.Verify(tokenString)
		if err != nil {
			reason := "invalid token"
			if errors.Is(err, jwt.ErrTokenExpired) {
				reason = "token expired"
			}
			slog.Warn("gateway: token rejected", "path", r.URL.Path, "reason", reason, "error", err)
			writeUnauthorized(w, reason)
			return
		}

		r.Header.Set("X-User", claims.Subject)
		r.Header.Set("X-User-Role", claims.Role)

		// Also put the claims in the request's context, so the next middleware in the
		// gateway (RequireRole) can read them without parsing the token again.
		ctx := context.WithValue(r.Context(), claimsKey{}, claims)
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}

// writeUnauthorized sends a 401. The WWW-Authenticate header tells the client which
// kind of credentials to send (required by the HTTP spec for 401 responses).
func writeUnauthorized(w http.ResponseWriter, msg string) {
	w.Header().Set("WWW-Authenticate", `Bearer realm="opspilot"`)
	writeError(w, http.StatusUnauthorized, msg)
}

func writeError(w http.ResponseWriter, status int, msg string) {
	writeJSON(w, status, map[string]string{"error": msg})
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(v)
}

// claimsKey is the context key for the verified claims. An unexported type means no
// other package can read or overwrite the value by accident.
type claimsKey struct{}

// ClaimsFrom returns the claims RequireAuth stored in the context.
func ClaimsFrom(ctx context.Context) (*auth.Claims, bool) {
	claims, ok := ctx.Value(claimsKey{}).(*auth.Claims)
	return claims, ok
}
