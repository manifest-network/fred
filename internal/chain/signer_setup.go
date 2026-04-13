package chain

import (
	"context"
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

// EnsureGrants creates any missing authz grants from the provider (granter)
// to each sub-signer (grantee) for the billing message types.
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
				return fmt.Errorf("failed to query grant for %s → %s (%s): %w", providerAddr, granteeAddr, msgType, err)
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
