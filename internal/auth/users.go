package auth

import (
	"fmt"

	"golang.org/x/crypto/bcrypt"
)

// Roles. 5c uses them to decide what each user may do.
const (
	RoleViewer    = "viewer"    // read only
	RoleResponder = "responder" // read, create and update
	RoleAdmin     = "admin"     // everything, including delete
)

type User struct {
	Username     string
	Role         string
	passwordHash []byte // bcrypt hash; the password itself is never stored
}

// UserStore checks usernames and passwords. It's in memory for now; a real system would
// keep users in a database or use an identity provider such as Okta or Keycloak.
type UserStore struct {
	users     map[string]User
	dummyHash []byte // compared against for unknown usernames, see Authenticate
}

// NewDemoUsers creates one user per role. All demo passwords are "password".
func NewDemoUsers() (*UserStore, error) {
	s := &UserStore{users: map[string]User{}}
	demo := []struct{ username, role string }{
		{"alice", RoleViewer},
		{"bob", RoleResponder},
		{"carol", RoleAdmin},
	}
	for _, d := range demo {
		hash, err := bcrypt.GenerateFromPassword([]byte("password"), bcrypt.DefaultCost)
		if err != nil {
			return nil, fmt.Errorf("hash password for %s: %w", d.username, err)
		}
		s.users[d.username] = User{Username: d.username, Role: d.role, passwordHash: hash}
	}
	s.dummyHash = s.users["alice"].passwordHash
	return s, nil
}

// Authenticate returns the user if the username exists and the password matches.
func (s *UserStore) Authenticate(username, password string) (User, bool) {
	user, found := s.users[username]
	hash := user.passwordHash
	if !found {
		// Still run bcrypt, so a wrong username takes as long as a wrong password.
		// Otherwise an attacker could time the responses to find out which usernames exist.
		hash = s.dummyHash
	}
	err := bcrypt.CompareHashAndPassword(hash, []byte(password))
	if !found || err != nil {
		return User{}, false
	}
	return user, true
}
