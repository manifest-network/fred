package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"strings"

	"github.com/manifest-network/fred/internal/backend/docker"
	"github.com/manifest-network/fred/internal/backend/shared"
)

type dockerEffectsMode uint8

const (
	dockerEffectsNone dockerEffectsMode = iota
	dockerEffectsInspect
	dockerEffectsRepair
)

// dockerEffectsCommand is assembled only by startup parsing. Repair carries
// both explicit operator inputs; it cannot silently become normal startup.
type dockerEffectsCommand struct {
	mode            dockerEffectsMode
	acknowledgement string
	backup          string
}

func parseDockerEffectsCommand(inspect, repair bool, acknowledgement, backup string, repairArgumentsSupplied bool) (dockerEffectsCommand, error) {
	if !repair && repairArgumentsSupplied {
		return dockerEffectsCommand{}, errors.New("-docker-effects-acknowledgement and -docker-effects-backup require -repair-unsettled-docker-effects")
	}
	switch {
	case repair:
		if strings.TrimSpace(acknowledgement) == "" || strings.TrimSpace(backup) == "" {
			return dockerEffectsCommand{}, errors.New("-repair-unsettled-docker-effects requires -docker-effects-acknowledgement and -docker-effects-backup")
		}
		return dockerEffectsCommand{mode: dockerEffectsRepair, acknowledgement: acknowledgement, backup: backup}, nil
	case inspect:
		return dockerEffectsCommand{mode: dockerEffectsInspect}, nil
	default:
		return dockerEffectsCommand{}, nil
	}
}

func (command dockerEffectsCommand) run(ctx context.Context, cfg docker.Config, logger *slog.Logger, output io.Writer) error {
	switch command.mode {
	case dockerEffectsInspect:
		inspection, err := docker.InspectUnsettledDockerEffectsForConfig(ctx, cfg, logger)
		if err != nil {
			return err
		}
		if err := writeDockerEffectsResult(output, inspection); err != nil {
			return fmt.Errorf("write Docker-effects inspection: %w", err)
		}
	case dockerEffectsRepair:
		result, repairErr := docker.RepairUnsettledDockerEffectsForConfig(ctx, cfg, logger, command.acknowledgement, command.backup)
		return writeDockerEffectsRepairOutcome(output, result, repairErr)
	default:
		return errors.New("offline Docker-effects command requires inspection or repair mode")
	}
	return nil
}

func writeDockerEffectsRepairOutcome(output io.Writer, result shared.DockerRecoveryRepairResult, repairErr error) error {
	// A failed repair may already have created its backup or committed its
	// journal update. Preserve that report while still returning failure. A
	// final probe-close error must not publish an unconditional success verdict.
	if repairErr != nil && result.Verdict == "DOCKER_EFFECTS_FENCED" {
		result.Verdict = "REPAIR_COMMITTED"
	}
	if repairErr == nil && result.Verdict != "DOCKER_EFFECTS_FENCED" {
		repairErr = errors.New("offline Docker-effects repair did not confirm its fenced postcondition")
	}
	writeErr := writeDockerEffectsResult(output, result)
	if writeErr != nil {
		writeErr = fmt.Errorf("write Docker-effects repair result: %w", writeErr)
	}
	return errors.Join(repairErr, writeErr)
}

func writeDockerEffectsResult(output io.Writer, result any) error {
	data, err := json.Marshal(result)
	if err != nil {
		return err
	}
	data = append(data, '\n')
	written, err := output.Write(data)
	if err == nil && written != len(data) {
		err = io.ErrShortWrite
	}
	return err
}
