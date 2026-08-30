package info

import (
	"crypto/rand"
	"encoding/hex"
)

var (
	Role             = "master"
	MasterReplID     = ""
	MasterReplOffset = 0
)

func SetRole(roleToSet string) {
	Role = roleToSet
}

const masterReplIDLength = 40

func GenerateMasterReplID() string {
	bytes := make([]byte, masterReplIDLength/2)
	_, err := rand.Read(bytes)
	if err != nil {
		panic(err)
	}
	return hex.EncodeToString(bytes)
}
