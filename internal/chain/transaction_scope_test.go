package chain

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	codectypes "github.com/cosmos/cosmos-sdk/codec/types"
	sdk "github.com/cosmos/cosmos-sdk/types"
	"github.com/cosmos/cosmos-sdk/types/tx"
	authtypes "github.com/cosmos/cosmos-sdk/x/auth/types"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
)

func acquireSignerForTest(t *testing.T, pool *SignerPool) (*Signer, bool, func()) {
	t.Helper()
	signer, isSub, release, err := pool.acquire(t.Context(), false)
	require.NoError(t, err)
	return signer, isSub, release
}

// Existing gas/retry fixtures exercise the same complete, serialized primary
// workflow as production while selecting their fixture's immutable signer.
func broadcastWithSignerForTest(c *Client, ctx context.Context, signer *Signer, msgs []sdk.Msg, opts broadcastOpts) (string, error) {
	if c.signerPool == nil || c.signerPool.primary != signer {
		return "", errors.New("transaction fixture signer differs from its client")
	}
	return c.broadcastPrimary(ctx, msgs, opts)
}

func TestSignerPool_WaitIsCancelable(t *testing.T) {
	for _, sub := range []bool{false, true} {
		t.Run(fmt.Sprint("sub=", sub), func(t *testing.T) {
			pool := newTestSignerPoolFromSigner(&Signer{})
			if sub {
				pool.subSigners = []*Signer{{}}
			}
			_, _, release, err := pool.acquire(t.Context(), false)
			require.NoError(t, err)
			defer release()
			ctx, cancel := context.WithTimeout(t.Context(), 20*time.Millisecond)
			defer cancel()
			_, _, _, err = pool.acquire(ctx, false)
			require.ErrorIs(t, err, context.DeadlineExceeded)
			release()
			_, _, nextRelease, err := pool.acquire(t.Context(), false)
			require.NoError(t, err)
			nextRelease()
		})
	}
}

func TestSignerPool_PrimaryAndFallbackShareSignerOwnership(t *testing.T) {
	primary := &Signer{}
	primaryPool := newTestSignerPoolFromSigner(primary)
	fallbackPool := newTestSignerPoolFromSigner(primary)
	_, _, release, err := primaryPool.acquire(t.Context(), true)
	require.NoError(t, err)
	defer release()
	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Millisecond)
	defer cancel()
	entered := false
	err = fallbackPool.withSigner(ctx, false, func(heldSigner) error {
		entered = true
		return nil
	})
	require.ErrorIs(t, err, context.DeadlineExceeded)
	require.False(t, entered, "a second pool cannot issue a transaction from a held account")
	release()
	require.NoError(t, fallbackPool.withSigner(t.Context(), false, func(heldSigner) error { return nil }))
}

func TestClient_WriteBudgetCoversEveryRPCPhase(t *testing.T) {
	for _, phase := range []string{"account", "simulate", "broadcast", "withdraw response"} {
		t.Run(phase, func(t *testing.T) {
			c, broadcasts := setupTxMocks(t)
			c.txTimeout = 150 * time.Millisecond
			ts := c.txService.(*mockTxService)
			switch phase {
			case "account":
				c.authQuery.(*mockAuthQuery).AccountFn = func(ctx context.Context, _ *authtypes.QueryAccountRequest, _ ...grpc.CallOption) (*authtypes.QueryAccountResponse, error) {
					<-ctx.Done()
					return nil, ctx.Err()
				}
			case "simulate":
				ts.SimulateFn = func(ctx context.Context, _ *tx.SimulateRequest, _ ...grpc.CallOption) (*tx.SimulateResponse, error) {
					<-ctx.Done()
					return nil, ctx.Err()
				}
			case "broadcast":
				ts.BroadcastTxFn = func(ctx context.Context, _ *tx.BroadcastTxRequest, _ ...grpc.CallOption) (*tx.BroadcastTxResponse, error) {
					broadcasts.Add(1)
					<-ctx.Done()
					return nil, ctx.Err()
				}
			case "withdraw response":
				getTx := ts.GetTxFn
				calls := 0
				ts.GetTxFn = func(ctx context.Context, req *tx.GetTxRequest, opts ...grpc.CallOption) (*tx.GetTxResponse, error) {
					calls++
					if calls == 1 {
						return getTx(ctx, req, opts...)
					}
					<-ctx.Done()
					return nil, ctx.Err()
				}
			}
			start := time.Now()
			var err error
			if phase == "withdraw response" {
				_, _, err = c.WithdrawByProvider(t.Context(), "provider", nil)
			} else {
				_, err = c.broadcastTx(t.Context(), newTestMsg(c.providerAddress))
			}
			require.ErrorIs(t, err, context.DeadlineExceeded)
			require.Less(t, time.Since(start), 2*time.Second)
			if phase == "account" || phase == "simulate" {
				require.Zero(t, broadcasts.Load())
			}
			if phase == "broadcast" {
				require.Equal(t, int32(1), broadcasts.Load())
			}
		})
	}
}

func TestClient_WriteBudgetIncludesSignerWait(t *testing.T) {
	c, broadcasts := setupTxMocks(t)
	c.txTimeout = 150 * time.Millisecond
	_, _, release, err := c.signerPool.acquire(t.Context(), true)
	require.NoError(t, err)
	defer release()
	_, err = c.broadcastTx(t.Context(), newTestMsg(c.providerAddress))
	require.ErrorIs(t, err, context.DeadlineExceeded)
	require.Zero(t, broadcasts.Load())
	release()
	c.txTimeout = time.Second
	_, err = c.broadcastTx(t.Context(), newTestMsg(c.providerAddress))
	require.NoError(t, err)
	require.Equal(t, int32(1), broadcasts.Load())
}

// Single-attempt protocol fixtures retain account-sequence ownership while
// bypassing only the retry policy they are intended to test independently.
func doBroadcastWithSignerForTest(c *Client, ctx context.Context, signer *Signer, msgs []sdk.Msg, seqOverride, gasOverride *uint64, preAccount *codectypes.Any) (hash string, err error) {
	if c.signerPool == nil || c.signerPool.primary != signer {
		return "", errors.New("transaction fixture signer differs from its client")
	}
	err = c.signerPool.withSigner(ctx, true, func(held heldSigner) error {
		var err error
		hash, err = c.doBroadcastTxWithSigner(ctx, held, msgs, seqOverride, gasOverride, preAccount)
		return err
	})
	return hash, err
}

func TestClient_WriteBudgetPreservesCommittedSubBatches(t *testing.T) {
	c, broadcasts := setupTxMocks(t)
	c.txTimeout = 200 * time.Millisecond
	ts := c.txService.(*mockTxService)
	broadcast := ts.BroadcastTxFn
	ts.BroadcastTxFn = func(ctx context.Context, req *tx.BroadcastTxRequest, opts ...grpc.CallOption) (*tx.BroadcastTxResponse, error) {
		if broadcasts.Load() != 0 {
			<-ctx.Done()
			return nil, ctx.Err()
		}
		return broadcast(ctx, req, opts...)
	}
	ids := make([]string, maxLeasesPerBatch+1)
	for i := range ids {
		ids[i] = fmt.Sprint("lease-", i)
	}
	processed, hashes, err := c.AcknowledgeLeases(t.Context(), ids)
	require.ErrorIs(t, err, context.DeadlineExceeded)
	require.Equal(t, uint64(maxLeasesPerBatch), processed)
	require.Equal(t, []string{"TX1"}, hashes)
	require.Equal(t, int32(1), broadcasts.Load())
}
