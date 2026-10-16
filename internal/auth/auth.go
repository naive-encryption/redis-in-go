package auth

import (
	"crypto/sha256"
	"encoding/hex"
)

type User struct {
	Name      string
	Flags     map[string]bool
	Passwords []string
}

var Users map[string]*User

func init() {
	Users = make(map[string]*User)
	Users["default"] = NewUser()
}

func NewUser() *User {
	user := User{Name: "default", Flags: make(map[string]bool), Passwords: make([]string, 0, 16)}
	user.Flags["nopass"] = true
	return &user
}

func GenerateSHA(input string) string {
	sum := sha256.Sum256([]byte(input))
	hexStr := hex.EncodeToString(sum[:])
	return hexStr
}

func (u *User) AddPassword(pass string) {
	u.Passwords = append(u.Passwords, GenerateSHA(pass))
	delete(u.Flags, "nopass")
}

func (u *User) Authorize(password string) bool {
	hashed := GenerateSHA(password)
	for _, pass := range u.Passwords {
		if pass == hashed {
			return true
		}
	}
	return false
}
