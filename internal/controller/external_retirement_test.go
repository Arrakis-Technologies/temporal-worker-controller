package controller

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	api "github.com/temporalio/temporal-worker-controller/api/v1alpha1"
	"github.com/temporalio/temporal-worker-controller/internal/k8s"
	apps "k8s.io/api/apps/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

func TestExternalRetirementStalePlanAndAcknowledgement(t *testing.T) {
	ctx := context.Background()
	worker := &api.WorkerDeployment{ObjectMeta: meta.ObjectMeta{Name: "pool", Namespace: "test", UID: "pool-uid", Generation: 2}}
	worker.Spec.SunsetStrategy.RetirementPolicy = api.WorkerRetirementExternal
	worker.Spec.SunsetStrategy.Retirements = []api.WorkerVersionRetirement{{BuildID: "B", RequestID: "job"}}
	deployment := &apps.Deployment{ObjectMeta: meta.ObjectMeta{Name: "old-b", Namespace: "test", Labels: map[string]string{k8s.WorkerDeploymentNameLabel: "pool", k8s.BuildIDLabel: "B"}}}
	r, _ := newTestReconciler([]client.Object{worker, deployment})
	require.NoError(t, r.acknowledgeRetirement(ctx, worker, &plan{}))
	require.Empty(t, worker.Status.Retirement.RetiredVersions, "present original worker cannot be acknowledged retired")
	require.Equal(t, int64(2), worker.Status.Retirement.ObservedGeneration)
	require.NoError(t, r.Delete(ctx, deployment))
	require.NoError(t, r.acknowledgeRetirement(ctx, worker, &plan{}))
	require.Equal(t, worker.Spec.SunsetStrategy.Retirements, worker.Status.Retirement.RetiredVersions)
	stale := worker.DeepCopy()
	stale.Spec.SunsetStrategy.RetirementPolicy = api.WorkerRetirementAutomatic
	require.True(t, apierrors.IsConflict(r.checkRetirementPlan(ctx, stale)), "pre-enrollment Automatic plan must not bypass new authority")
	stale = worker.DeepCopy()
	stale.Generation--
	require.True(t, apierrors.IsConflict(r.checkRetirementPlan(ctx, stale)), "withdrawn authorization invalidates old plans")
	stale = worker.DeepCopy()
	stale.UID = "recreated"
	require.True(t, apierrors.IsConflict(r.checkRetirementPlan(ctx, stale)))
	worker.Status.TargetVersion.BuildID = "B"
	require.NoError(t, r.acknowledgeRetirement(ctx, worker, &plan{}))
	require.Empty(t, worker.Status.Retirement.RetiredVersions, "missing target does not grant retirement")
}
