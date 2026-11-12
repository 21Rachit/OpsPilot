package auth

// rank orders the roles: a higher role can do everything a lower one can.
var rank = map[string]int{
	RoleViewer:    1,
	RoleResponder: 2,
	RoleAdmin:     3,
}

// HasAtLeast reports whether role is min or higher. An unknown role has no rights at all.
func HasAtLeast(role, min string) bool {
	r, ok := rank[role]
	return ok && r >= rank[min]
}
