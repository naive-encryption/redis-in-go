package auth

type User struct {
	Name      string
	Flags     map[string]bool
	Passwords []string
}

func NewUser() *User {
	return &User{Name: "default", Flags: make(map[string]bool), Passwords: make([]string, 16)}
}
