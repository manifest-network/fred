package shared

import (
	"context"

	bolt "go.etcd.io/bbolt"
)

// walkCallbackValidationRows keeps cancellation in the synchronous owner of
// the read transaction. Every visited row is still fully decoded and checked;
// cancellation abandons the health result rather than returning partial proof.
func walkCallbackValidationRows(ctx context.Context, bucket *bolt.Bucket, visit func(key, value []byte) error) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := bucket.ForEach(func(key, value []byte) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		return visit(key, value)
	}); err != nil {
		return err
	}
	return ctx.Err()
}
