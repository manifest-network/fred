package shared

import "github.com/manifest-network/fred/internal/operationid"

func mustSharedOperationID(text string) OperationID {
	id, err := operationid.Parse(text)
	if err != nil {
		panic(err)
	}
	return id
}
