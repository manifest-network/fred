package main

import "github.com/manifest-network/fred/internal/hmacauth"

// testRequestKeys verifies requests signed with testSecret.
var testRequestKeys = mustRequestKeys(testSecret, "")

func mustRequestKeys(current, next string) hmacauth.VerifyKeys {
	keys, err := hmacauth.NewVerifyKeys(current, next)
	if err != nil {
		panic(err)
	}
	return keys
}
