package main

import (
	"net"
	"crypto/sha256"
	"encoding/hex"
)

var loggedInUsers = make(map[net.Conn]string)

type aclUser struct {
    Username string
    Flags    map[string]struct{}
    Password map[string]struct{}
}

func newAclUser(username string) aclUser {
    user := aclUser{
        Username: username,
        Flags: map[string]struct{}{
            "nopass": {},
        },
        Password: make(map[string]struct{}),
    }
	if config.Users == nil {
		config.Users = make(map[string]aclUser)
	}
    config.Users[username] = user
    return user
}

func mapToSlice(mp map[string]struct{}) []RespValue {
    properties := []RespValue{}
    for property := range mp {
        properties = append(properties, RespValue{BULK, property})
    }
	return properties
}

func (user aclUser) toGetUser() []RespValue {
    return []RespValue{
        {Type: BULK, Value: "flags"},
        {Type: ARRAY, Value: mapToSlice(user.Flags)},
        {Type: BULK, Value: "passwords"},
        {Type: ARRAY, Value:  mapToSlice(user.Password)},
    }
}

func (user aclUser) setPassword(raw string) {
    if len(raw) > 0 && raw[0] == '>' {
        raw = raw[1:]
    }

    passwordHash := generatePasswordHash(raw)

    user.Password[passwordHash] = struct{}{}
    delete(user.Flags, "nopass")
}

func (user aclUser) authenticate(conn net.Conn, password string) bool {
    passwordHash := generatePasswordHash(password)
    _, passwordCorrect := user.Password[passwordHash]
    _, nopassSet := user.Flags["nopass"]
    
    if !passwordCorrect && !nopassSet {
        return false
    }
    loggedInUsers[conn] = user.Username
    return true
}

func generatePasswordHash(password string) string {
    sum := sha256.Sum256([]byte(password))
    return hex.EncodeToString(sum[:])
}