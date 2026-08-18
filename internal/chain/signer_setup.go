package chain

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"cosmossdk.io/math"
	sdk "github.com/cosmos/cosmos-sdk/types"
	"github.com/cosmos/cosmos-sdk/x/authz"
	banktypes "github.com/cosmos/cosmos-sdk/x/bank/types"
	"google.golang.org/grpc"

	billingtypes "github.com/manifest-network/manifest-ledger/x/billing/types"
)

// grantedMsgTypes are the message types sub-signers are authorized to execute.
var grantedMsgTypes = []string{
	sdk.MsgTypeURL(&billingtypes.MsgAcknowledgeLease{}),
	sdk.MsgTypeURL(&billingtypes.MsgRejectLease{}),
	sdk.MsgTypeURL(&billingtypes.MsgCloseLease{}),
}

// ErrGrantsUnverified reports that the grant *queries* failed, so nothing is
// known about the grants. It is deliberately distinct from a failure to create
// a grant we positively determined was missing.
//
// The distinction is load-bearing (ENG-688). Grants are created with
// expiration=nil below, so they are permanent: an unreadable chain says nothing
// about whether they exist. Treating a failed read as "absent" and demoting the
// signer pool on it cost dev three weeks of single-signer operation after a
// single startup-race RPC timeout — the nine grants were on chain the whole
// time. Callers must keep the sub-signers on this error and let the periodic
// sub-signer maintenance sweep converge; only a positive "the grants are
// missing and could not be created" answer justifies falling back.
var ErrGrantsUnverified = errors.New("authz grants could not be verified")

// Retry configuration for EnsureGrantsWithRetry, mirroring the withdrawal
// scheduler's transient-failure ladder (internal/scheduler/withdraw.go).
const (
	grantMaxRetries     = 3
	grantInitialBackoff = 1 * time.Second
	grantMaxBackoff     = 10 * time.Second
)

// authzQuerier queries existing authz grants.
type authzQuerier interface {
	Grants(ctx context.Context, in *authz.QueryGrantsRequest, opts ...grpc.CallOption) (*authz.QueryGrantsResponse, error)
}

// bankQuerier queries account balances.
type bankQuerier interface {
	Balance(ctx context.Context, in *banktypes.QueryBalanceRequest, opts ...grpc.CallOption) (*banktypes.QueryBalanceResponse, error)
}

// txBroadcaster broadcasts transactions via the primary signer.
type txBroadcaster interface {
	broadcastTx(ctx context.Context, msg sdk.Msg) (string, error)
	broadcastMultiMsgTx(ctx context.Context, msgs []sdk.Msg) (string, error)
}

// EnsureGrantsWithRetry runs EnsureGrants, retrying transient failures with
// exponential backoff. ctx is the TOTAL budget: every attempt inherits whatever
// is left of it, and the loop abandons early once it is done.
//
// Retries are unconditional — there is deliberately no transient/permanent
// classification, matching the withdrawal scheduler's retry loop. Classifying
// by gRPC status code cannot work here: the authz module registers
// ErrNoAuthorizationFound with cosmossdk.io/errors.Register (x/authz/errors.go
// — note that is NOT this file's stdlib "errors" import), and that form defaults
// the gRPC code to codes.Unknown rather than codes.NotFound, while
// Client.isRetryableGRPCCode counts Unknown as retryable. So a "grant missing"
// answer and an RPC blip are indistinguishable by code.
// grantExists already folds a genuine not-found into (false, nil) before the
// loop can see it, so every error reaching here is either transient or a
// deterministic configuration error that costs three cheap attempts.
//
// EnsureGrants re-queries existence on every run, so a retry is always safe:
// grants created by an attempt whose response was lost are simply skipped.
func EnsureGrantsWithRetry(ctx context.Context, authzQ authzQuerier, broadcaster txBroadcaster, pool *SignerPool) error {
	var err error
	backoff := grantInitialBackoff

	for attempt := 1; attempt <= grantMaxRetries; attempt++ {
		err = EnsureGrants(ctx, authzQ, broadcaster, pool)
		if err == nil {
			return nil
		}

		if attempt == grantMaxRetries {
			return fmt.Errorf("ensure authz grants after %d attempts: %w", attempt, err)
		}

		slog.Warn("authz grant setup failed, retrying",
			"error", err,
			"attempt", attempt,
			"backoff", backoff,
		)

		select {
		case <-ctx.Done():
			// Preserve the grant error rather than the context error: the
			// caller branches on ErrGrantsUnverified, and a bare ctx error
			// would lose that verdict and fall through to the demotion path.
			return fmt.Errorf("ensure authz grants after %d attempts (%w): %w", attempt, ctx.Err(), err)
		case <-time.After(backoff):
		}

		backoff *= 2
		if backoff > grantMaxBackoff {
			backoff = grantMaxBackoff
		}
	}

	return err
}

// EnsureGrants creates any missing authz grants from the provider (granter)
// to each sub-signer (grantee) for the billing message types.
//
// A failure to QUERY a grant is reported as ErrGrantsUnverified; every other
// error is reached only after the queries succeeded and so carries the positive
// fact that a grant is genuinely missing. Callers branch on that distinction.
func EnsureGrants(ctx context.Context, authzQ authzQuerier, broadcaster txBroadcaster, pool *SignerPool) error {
	providerAddr := pool.ProviderAddress()
	subAddrs := pool.SubSignerAddresses()
	if len(subAddrs) == 0 {
		return nil
	}

	var missingGrants []sdk.Msg
	for _, granteeAddr := range subAddrs {
		for _, msgType := range grantedMsgTypes {
			exists, err := grantExists(ctx, authzQ, providerAddr, granteeAddr, msgType)
			if err != nil {
				return fmt.Errorf("%w: failed to query grant for %s → %s (%s): %w",
					ErrGrantsUnverified, providerAddr, granteeAddr, msgType, err)
			}
			if exists {
				continue
			}

			granter, err := sdk.AccAddressFromBech32(providerAddr)
			if err != nil {
				return fmt.Errorf("invalid granter address: %w", err)
			}
			grantee, err := sdk.AccAddressFromBech32(granteeAddr)
			if err != nil {
				return fmt.Errorf("invalid grantee address: %w", err)
			}

			// expiration=nil → permanent grant (revocable only via MsgRevoke).
			// blockTime is unused when expiration is nil; zero value is fine.
			grant, err := authz.NewGrant(time.Time{}, authz.NewGenericAuthorization(msgType), nil)
			if err != nil {
				return fmt.Errorf("failed to create grant: %w", err)
			}
			msg := &authz.MsgGrant{
				Granter: granter.String(),
				Grantee: grantee.String(),
				Grant:   grant,
			}
			missingGrants = append(missingGrants, msg)
		}
	}

	if len(missingGrants) == 0 {
		slog.Info("all authz grants already exist", "sub_signers", len(subAddrs))
		return nil
	}

	slog.Info("creating authz grants", "count", len(missingGrants))

	// Try batched first
	if _, err := broadcaster.broadcastMultiMsgTx(ctx, missingGrants); err != nil {
		slog.Warn("batched grant tx failed, falling back to individual", "error", err)
		for _, msg := range missingGrants {
			if _, err := broadcaster.broadcastTx(ctx, msg); err != nil {
				return fmt.Errorf("failed to create grant: %w", err)
			}
		}
	}

	slog.Info("authz grants created", "count", len(missingGrants))
	return nil
}

// grantExists checks if a specific authz grant exists.
func grantExists(ctx context.Context, authzQ authzQuerier, granter, grantee, msgType string) (bool, error) {
	resp, err := authzQ.Grants(ctx, &authz.QueryGrantsRequest{
		Granter:    granter,
		Grantee:    grantee,
		MsgTypeUrl: msgType,
	})
	if err != nil {
		// The authz module returns an error (codespace authz, code 2) when no
		// grant exists, rather than an empty list. Treat as "not found".
		if strings.Contains(err.Error(), "authorization not found") {
			return false, nil
		}
		return false, err
	}
	return len(resp.Grants) > 0, nil
}

// EnsureFunding tops up sub-signers that are below the minimum balance.
func EnsureFunding(ctx context.Context, bankQ bankQuerier, broadcaster txBroadcaster, pool *SignerPool, minBalance, topUpAmount sdk.Coin) error {
	providerAddr := pool.ProviderAddress()
	subAddrs := pool.SubSignerAddresses()
	if len(subAddrs) == 0 {
		return nil
	}

	// Check provider balance first
	providerBal, err := bankQ.Balance(ctx, &banktypes.QueryBalanceRequest{
		Address: providerAddr,
		Denom:   minBalance.Denom,
	})
	if err != nil {
		return fmt.Errorf("failed to query provider balance: %w", err)
	}

	var needsFunding []string
	for _, addr := range subAddrs {
		bal, err := bankQ.Balance(ctx, &banktypes.QueryBalanceRequest{
			Address: addr,
			Denom:   minBalance.Denom,
		})
		if err != nil {
			slog.Warn("failed to query sub-signer balance", "address", addr, "error", err)
			continue
		}
		if bal.Balance.Amount.LT(minBalance.Amount) {
			needsFunding = append(needsFunding, addr)
		}
	}

	if len(needsFunding) == 0 {
		slog.Info("all sub-signers funded", "sub_signers", len(subAddrs))
		return nil
	}

	// Check provider can afford the top-ups
	totalNeeded := topUpAmount.Amount.Mul(math.NewInt(int64(len(needsFunding))))
	gasReserve := math.NewInt(1_000_000) // 1 MFX reserve for gas
	if providerBal.Balance.Amount.LT(totalNeeded.Add(gasReserve)) {
		slog.Warn("provider balance too low for sub-signer funding",
			"provider_balance", providerBal.Balance,
			"needed", totalNeeded,
			"sub_signers_needing_funding", len(needsFunding),
		)
		return fmt.Errorf("provider balance %s insufficient to fund %d sub-signers (need %s + gas reserve)",
			providerBal.Balance, len(needsFunding), sdk.NewCoin(minBalance.Denom, totalNeeded))
	}

	var sendMsgs []sdk.Msg
	fromAddr, err := sdk.AccAddressFromBech32(providerAddr)
	if err != nil {
		return fmt.Errorf("invalid provider address %s: %w", providerAddr, err)
	}
	for _, addr := range needsFunding {
		toAddr, err := sdk.AccAddressFromBech32(addr)
		if err != nil {
			return fmt.Errorf("invalid sub-signer address %s: %w", addr, err)
		}
		sendMsgs = append(sendMsgs, banktypes.NewMsgSend(
			fromAddr, toAddr, sdk.NewCoins(topUpAmount),
		))
	}

	slog.Info("funding sub-signers", "count", len(needsFunding), "amount_each", topUpAmount)

	if _, err := broadcaster.broadcastMultiMsgTx(ctx, sendMsgs); err != nil {
		slog.Warn("batched funding tx failed, falling back to individual", "error", err)
		for _, msg := range sendMsgs {
			if _, err := broadcaster.broadcastTx(ctx, msg); err != nil {
				return fmt.Errorf("failed to fund sub-signer: %w", err)
			}
		}
	}

	slog.Info("sub-signers funded", "count", len(needsFunding))
	return nil
}
