// Unless explicitly stated otherwise all files in this repository are licensed under the MIT License.
//
// This product includes software developed at Datadog, Inc. Copyright 2024 Datadog, Inc.

package controller

import (
	"testing"
	"time"
)

func TestWorkerDeploymentReconcilerMaxConcurrentReconciles(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		configured int
		want       int
	}{
		{
			name: "default",
			want: DefaultMaxConcurrentReconciles,
		},
		{
			name:       "configured",
			configured: 5,
			want:       5,
		},
		{
			name:       "negative uses default",
			configured: -1,
			want:       DefaultMaxConcurrentReconciles,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			reconciler := &WorkerDeploymentReconciler{
				MaxConcurrentReconciles: tt.configured,
			}
			if got := reconciler.maxConcurrentReconciles(); got != tt.want {
				t.Fatalf("maxConcurrentReconciles() = %d, want %d", got, tt.want)
			}
		})
	}
}

func TestWorkerDeploymentReconcilerControllerOptionsUseConfiguredConcurrency(t *testing.T) {
	t.Parallel()

	recoverPanic := true
	reconciler := &WorkerDeploymentReconciler{MaxConcurrentReconciles: 3}
	options := reconciler.controllerOptions(&recoverPanic)

	if options.MaxConcurrentReconciles != 3 {
		t.Fatalf("MaxConcurrentReconciles = %d, want 3", options.MaxConcurrentReconciles)
	}
	if options.RecoverPanic != &recoverPanic {
		t.Fatal("RecoverPanic pointer was not preserved")
	}
}

func TestWorkerDeploymentReconcilerReconcileInterval(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		configured time.Duration
		want       time.Duration
	}{
		{
			name: "default",
			want: DefaultReconcileInterval,
		},
		{
			name:       "configured",
			configured: 2 * time.Minute,
			want:       2 * time.Minute,
		},
		{
			name:       "negative uses default",
			configured: -time.Second,
			want:       DefaultReconcileInterval,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			reconciler := &WorkerDeploymentReconciler{
				ReconcileInterval: tt.configured,
			}
			if got := reconciler.reconcileInterval(); got != tt.want {
				t.Fatalf("reconcileInterval() = %s, want %s", got, tt.want)
			}
		})
	}
}

func TestWorkerDeploymentReconcilerSuccessfulResultUsesConfiguredInterval(t *testing.T) {
	t.Parallel()

	reconciler := &WorkerDeploymentReconciler{ReconcileInterval: 2 * time.Minute}
	result := reconciler.successfulReconcileResult()

	if !result.Requeue {
		t.Fatal("successful reconcile must requeue")
	}
	if result.RequeueAfter != 2*time.Minute {
		t.Fatalf("RequeueAfter = %s, want 2m", result.RequeueAfter)
	}
}
