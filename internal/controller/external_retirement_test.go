package controller

import (
	"context"
	"testing"

	"github.com/go-logr/logr"
	"github.com/stretchr/testify/require"
	api "github.com/temporalio/temporal-worker-controller/api/v1alpha1"
	"github.com/temporalio/temporal-worker-controller/internal/k8s"
	"github.com/temporalio/temporal-worker-controller/internal/planner"
	"github.com/temporalio/temporal-worker-controller/internal/temporal"
	deploymentpb "go.temporal.io/api/deployment/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/api/workflowservice/v1"
	apps "k8s.io/api/apps/v1"
	core "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

func TestExternalRetirementRequiresAuthoritativeTemporalObservation(t *testing.T) {
	for _, tc := range []struct {
		name     string
		response *workflowservice.DescribeWorkerDeploymentResponse
		err      error
		accepted bool
	}{
		{"missing deployment info", &workflowservice.DescribeWorkerDeploymentResponse{}, nil, false},
		{"missing routing info", &workflowservice.DescribeWorkerDeploymentResponse{WorkerDeploymentInfo: &deploymentpb.WorkerDeploymentInfo{}}, nil, false},
		{"request failure", nil, serviceerror.NewUnavailable("unavailable"), false},
		{"confirmed absent", nil, serviceerror.NewNotFound("absent"), true},
		{"confirmed empty", &workflowservice.DescribeWorkerDeploymentResponse{WorkerDeploymentInfo: &deploymentpb.WorkerDeploymentInfo{RoutingConfig: &deploymentpb.RoutingConfig{}}}, nil, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			stub := newStubTemporalClient(nil)
			stub.describeDeploymentErr, stub.describeDeploymentResponse = tc.err, tc.response
			d := &apps.Deployment{ObjectMeta: meta.ObjectMeta{Name: "worker-b", Namespace: "test", UID: "b-uid", ResourceVersion: "1", Labels: k8s.ComputeSelectorLabels("pool", "B")}}
			state := &k8s.DeploymentState{Deployments: map[string]*apps.Deployment{"B": d}, DeploymentRefs: map[string]*core.ObjectReference{"B": k8s.NewObjectRef(d)}}
			observed, err := temporal.GetWorkerDeploymentState(context.Background(), stub, "test/pool", "tenant", state.Deployments, "A", api.UpdateAllAtOnce, "controller")
			if !tc.accepted {
				require.Error(t, err, "an incomplete response must stop before planning")
				require.Nil(t, observed)
				return
			}
			require.NoError(t, err)
			worker, _, _ := retirementResourceFixture(t)
			worker.Status = *newStateMapper(state, observed, "test/pool").mapToStatus("A")
			p, err := planner.GeneratePlan(logr.Discard(), state, &worker.Status, &worker.Spec, observed, api.ConnectionSpec{}, &planner.Config{}, "test/pool", 75, nil, false, nil, worker.Name, worker.UID)
			require.NoError(t, err)
			require.Equal(t, []*apps.Deployment{d}, p.DeleteDeployments, "confirmed absence must allow the last authorized worker to retire")
		})
	}
}

func TestExternalRetirementStalePlanAndAcknowledgement(t *testing.T) {
	ctx := context.Background()
	worker := &api.WorkerDeployment{ObjectMeta: meta.ObjectMeta{Name: "pool", Namespace: "test", UID: "pool-uid", Generation: 2}}
	worker.Spec.SunsetStrategy.RetirementPolicy = api.WorkerRetirementExternal
	worker.Spec.SunsetStrategy.Retirements = []api.WorkerVersionRetirement{{BuildID: "B", RequestID: "job"}}
	deployment := &apps.Deployment{ObjectMeta: meta.ObjectMeta{Name: "old-b", Namespace: "test", Labels: map[string]string{k8s.WorkerDeploymentNameLabel: "pool", k8s.BuildIDLabel: "B"}}}
	r, _ := newTestReconciler([]client.Object{worker, deployment})
	fresh, _ := newTestReconciler([]client.Object{worker.DeepCopy(), deployment.DeepCopy()})
	r.APIReader = fresh.Client
	require.NoError(t, r.acknowledgeRetirement(ctx, worker, &plan{}))
	require.Empty(t, worker.Status.Retirement.RetiredVersions, "present original worker cannot be acknowledged retired")
	require.Equal(t, int64(2), worker.Status.Retirement.ObservedGeneration)
	require.NoError(t, r.Delete(ctx, deployment))
	require.NoError(t, r.acknowledgeRetirement(ctx, worker, &plan{}))
	require.Empty(t, worker.Status.Retirement.RetiredVersions, "cached absence cannot override a live worker")
	require.NoError(t, fresh.Delete(ctx, deployment))
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
	// The cached spec still contains the release request; only the uncached
	// reader sees its withdrawal. Old work must stop at that boundary.
	withdrawn := worker.DeepCopy()
	withdrawn.Generation++
	withdrawn.Spec.SunsetStrategy.Retirements = nil
	require.NoError(t, fresh.Update(ctx, withdrawn))
	require.True(t, apierrors.IsConflict(r.checkRetirementPlan(ctx, worker)))
}
