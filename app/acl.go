package main

import (
	"net"
)

var loggedInUsers = make(map[net.Conn]string)

type aclUser struct {
    Flags    map[string]struct{}
    Password map[string]struct{}
}

func newAclUser(username string) aclUser {
    user := aclUser{
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

func (user aclUser) toGetUser() []RespValue {
    return []RespValue{
        {Type: BULK, Value: "flags"},
        {Type: ARRAY, Value: mapToSlice(user.Flags)},
        {Type: BULK, Value: "passwords"},
        {Type: ARRAY, Value:  mapToSlice(user.Password)},
    }
}

func mapToSlice(mp map[string]struct{}) []RespValue {
    properties := []RespValue{}
    for property := range mp {
        properties = append(properties, RespValue{BULK, property})
    }
	return properties
}
