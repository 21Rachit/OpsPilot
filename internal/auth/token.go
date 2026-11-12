package auth

import (
	"errors"
	"fmt"
	"time"

	"github.com/golang-jwt/jwt/v5"
)

const issuer = "opspilot-gateway"

// Claims is what the token says about the user. RegisteredClaims adds the standard
// fields: sub (username), iss (who issued it), iat (issued at) and exp (expires at).
type Claims struct {
	Role string `json:"role"`
	jwt.RegisteredClaims
}

// TokenService issues and verifies JWTs signed with HMAC-SHA256 (HS256).
type TokenService struct {
	secret []byte
	ttl    time.Duration
}

func NewTokenService(secret string, ttl time.Duration) (*TokenService, error) {
	if len(secret) < 32 {
		return nil, errors.New("JWT_SECRET must be at least 32 characters")
	}
	return &TokenService{secret: []byte(secret), ttl: ttl}, nil
}

// Issue creates a signed token for the user. It returns the token and when it expires.
func (s *TokenService) Issue(user User) (string, time.Time, error) {
	now := time.Now()
	expires := now.Add(s.ttl)
	claims := Claims{
		Role: user.Role,
		RegisteredClaims: jwt.RegisteredClaims{
			Subject:   user.Username,
			Issuer:    issuer,
			IssuedAt:  jwt.NewNumericDate(now),
			ExpiresAt: jwt.NewNumericDate(expires),
		},
	}
	token, err := jwt.NewWithClaims(jwt.SigningMethodHS256, claims).SignedString(s.secret)
	if err != nil {
		return "", time.Time{}, fmt.Errorf("sign token: %w", err)
	}
	return token, expires, nil
}

// Verify checks the signature, issuer and expiry, and returns the claims if all are valid.
func (s *TokenService) Verify(tokenString string) (*Claims, error) {
	claims := &Claims{}
	_, err := jwt.ParseWithClaims(tokenString, claims,
		func(*jwt.Token) (any, error) { return s.secret, nil },
		// Only accept HS256. Without this, a token claiming "alg": "none" or another
		// algorithm could trick the library into skipping or weakening the check.
		jwt.WithValidMethods([]string{jwt.SigningMethodHS256.Alg()}),
		jwt.WithIssuer(issuer),
		jwt.WithExpirationRequired(),
	)
	if err != nil {
		return nil, err
	}
	return claims, nil
}
