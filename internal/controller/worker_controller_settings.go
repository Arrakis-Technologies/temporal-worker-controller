// Unless explicitly stated otherwise all files in this repository are licensed under the MIT License.
//
// This product includes software developed at Datadog, Inc. Copyright 2024 Datadog, Inc.

package controller

import (
	"time"

	"sigs.k8s.io/controller-runtime/pkg/controller"
)

const (
	DefaultMaxConcurrentReconciles = 100
	DefaultReconcileInterval       = 10 * time.Second
)

func (r *WorkerDeploymentReconciler) maxConcurrentReconciles() int {
	if r.MaxConcurrentReconciles > 0 {
		return r.MaxConcurrentReconciles
	}
	return DefaultMaxConcurrentReconciles
}

func (r *WorkerDeploymentReconciler) reconcileInterval() time.Duration {
	if r.ReconcileInterval > 0 {
		return r.ReconcileInterval
	}
	return DefaultReconcileInterval
}

func (r *WorkerDeploymentReconciler) controllerOptions(recoverPanic *bool) controller.Options {
	return controller.Options{
		MaxConcurrentReconciles: r.maxConcurrentReconciles(),
		RecoverPanic:            recoverPanic,
	}
}
