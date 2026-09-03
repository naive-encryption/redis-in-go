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

const (
	masterReplIDLength = 40
)

func GetEmptyRDB() []byte {
	hexRDB := "524544495330303131fa0972656469732d76657205372e322e30fa0a72656469732d62697473c040fa056374696d65c26d08bc65fa08757365642d6d656dc2b0c41000fa08616f662d62617365c000fff06e3bfec0ff5aa2"
	data, _ := hex.DecodeString(hexRDB)
	return data
}

func GenerateMasterReplID() string {
	bytes := make([]byte, masterReplIDLength/2)
	_, err := rand.Read(bytes)
	if err != nil {
		panic(err)
	}
	return hex.EncodeToString(bytes)
}
