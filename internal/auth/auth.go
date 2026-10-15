package auth

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
