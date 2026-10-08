// Unless explicitly stated otherwise all files in this repository are licensed under the MIT License.
//
// This product includes software developed at Datadog, Inc. Copyright 2024 Datadog, Inc.

package main

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestHQStartupFlags(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	manager := filepath.Join(t.TempDir(), "manager")
	build := exec.CommandContext(ctx, "go", "build", "-o", manager, "./main.go")
	if output, err := build.CombinedOutput(); err != nil {
		t.Fatalf("build controller: %v\n%s", err, output)
	}
	args := []string{
		"--leader-elect",
		"--metrics-bind-address=127.0.0.1:8080",
		"--health-probe-bind-address=:8081",
		"--max-concurrent-reconciles=1",
		"--reconcile-interval=5s",
	}
	t.Run("accepts exact HQ arguments", func(t *testing.T) {
		cmd := exec.CommandContext(ctx, manager, append(args, "--help")...)
		if output, err := cmd.CombinedOutput(); err != nil {
			t.Fatalf("controller must accept the unchanged HQ startup arguments: %v\n%s", err, output)
		}
	})
	t.Run("valid settings reach configuration loading", func(t *testing.T) {
		missingConfig := filepath.Join(t.TempDir(), "missing-kubeconfig")
		cmd := exec.CommandContext(ctx, manager, append(args, "--kubeconfig="+missingConfig)...)
		// Explicit missing configuration ensures this process cannot contact HQ or Kind.
		cmd.Env = append(os.Environ(), "KUBECONFIG="+missingConfig)
		output, err := cmd.CombinedOutput()
		if err == nil || !strings.Contains(string(output), missingConfig) {
			t.Fatalf("expected local configuration load after valid settings, got %v\n%s", err, output)
		}
	})
	for _, tc := range []struct {
		name, argument, message string
	}{
		{"zero concurrency", "--max-concurrent-reconciles=0", "max-concurrent-reconciles must be at least 1"},
		{"negative concurrency", "--max-concurrent-reconciles=-1", "max-concurrent-reconciles must be at least 1"},
		{"zero interval", "--reconcile-interval=0s", "reconcile-interval must be greater than zero"},
		{"negative interval", "--reconcile-interval=-1s", "reconcile-interval must be greater than zero"},
		{"malformed interval", "--reconcile-interval=not-a-duration", "invalid value"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cmd := exec.CommandContext(ctx, manager, append(args, tc.argument)...)
			output, err := cmd.CombinedOutput()
			if err == nil || !strings.Contains(string(output), tc.message) {
				t.Fatalf("must reject invalid settings before manager startup: %v\n%s", err, output)
			}
		})
	}
}
