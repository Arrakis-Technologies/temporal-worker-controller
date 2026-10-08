package planner

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	api "github.com/temporalio/temporal-worker-controller/api/v1alpha1"
	"github.com/temporalio/temporal-worker-controller/internal/k8s"
	apps "k8s.io/api/apps/v1"
	core "k8s.io/api/core/v1"
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// Exercise the same wire decoder and planner used in reconciliation. The wire
// fixture deliberately precedes adding the new fields: old binaries ignore them
// and reproduce the independent worker-loss defect, rather than failing to build.
func TestExternalRetirementSavedBuildSurvivesReusedSuccessor(t *testing.T) {
	var spec api.WorkerDeploymentSpec
	require.NoError(t, json.Unmarshal([]byte(`{
      "replicas":1,"sunset":{"retirementPolicy":"External","scaledownDelay":"0s","deleteDelay":"0s"}
    }`), &spec))
	replicas := int32(1)
	old := &apps.Deployment{ObjectMeta: meta.ObjectMeta{Name: "pool-b", Namespace: "test"}, Spec: apps.DeploymentSpec{Replicas: &replicas}}
	state := &k8s.DeploymentState{Deployments: map[string]*apps.Deployment{"B": old}}
	ref := &core.ObjectReference{Name: old.Name, Namespace: old.Namespace}
	status := &api.WorkerDeploymentStatus{
		TargetVersion: api.TargetWorkerDeploymentVersion{BaseWorkerDeploymentVersion: api.BaseWorkerDeploymentVersion{BuildID: "A"}},
		DeprecatedVersions: []*api.DeprecatedWorkerDeploymentVersion{{
			BaseWorkerDeploymentVersion: api.BaseWorkerDeploymentVersion{BuildID: "B", Status: api.VersionStatusInactive, Deployment: ref},
		}},
	}
	scales := getScaleDeployments(state, status, &spec)
	require.NotContains(t, scales, ref, "reused A must not scale saved B down before external release authority")
	status.DeprecatedVersions[0].Status = api.VersionStatusDrained
	status.DeprecatedVersions[0].DrainedSince = &meta.Time{Time: time.Now().Add(-time.Hour)}
	require.NotContains(t, getScaleDeployments(state, status, &spec), ref, "Temporal drainage alone cannot retire saved B")
	*old.Spec.Replicas = 0 // stale worker from before enrollment must not be erased
	require.Empty(t, getDeleteDeployments(state, status, &spec, true), "saved B must survive zero-replica bootstrap")
	status.DeprecatedVersions[0].Status = api.VersionStatusNotRegistered
	require.Empty(t, getDeleteDeployments(state, status, &spec, true), "transient registration loss must not erase saved B")
}

func TestExternalRetirementAuthorizationAndRouting(t *testing.T) {
	for _, policy := range []api.WorkerRetirementPolicy{api.WorkerRetirementAutomatic, api.WorkerRetirementExternal} {
		t.Run(string(policy), func(t *testing.T) {
			one := int32(1)
			zero := &meta.Duration{}
			deployment := &apps.Deployment{ObjectMeta: meta.ObjectMeta{Name: "b", Labels: map[string]string{k8s.BuildIDLabel: "B"}}, Spec: apps.DeploymentSpec{Replicas: &one}}
			state := &k8s.DeploymentState{Deployments: map[string]*apps.Deployment{"B": deployment}}
			ref := &core.ObjectReference{Name: "b"}
			status := &api.WorkerDeploymentStatus{TargetVersion: api.TargetWorkerDeploymentVersion{BaseWorkerDeploymentVersion: api.BaseWorkerDeploymentVersion{BuildID: "A"}}, DeprecatedVersions: []*api.DeprecatedWorkerDeploymentVersion{{BaseWorkerDeploymentVersion: api.BaseWorkerDeploymentVersion{BuildID: "B", Status: api.VersionStatusDrained, Deployment: ref}, DrainedSince: &meta.Time{Time: time.Now().Add(-time.Hour)}}}}
			spec := &api.WorkerDeploymentSpec{SunsetStrategy: api.SunsetStrategy{RetirementPolicy: policy, ScaledownDelay: zero, DeleteDelay: zero}}
			if policy == api.WorkerRetirementExternal {
				require.Empty(t, getScaleDeployments(state, status, spec))
				*deployment.Spec.Replicas = 0
				require.Equal(t, uint32(1), getScaleDeployments(state, status, spec)[ref], "restore original-image poller")
				*deployment.Spec.Replicas = 1
				spec.SunsetStrategy.Retirements = []api.WorkerVersionRetirement{{BuildID: "B", RequestID: "job"}}
			}
			require.Equal(t, uint32(0), getScaleDeployments(state, status, spec)[ref])
			*deployment.Spec.Replicas = 0
			require.Equal(t, []*apps.Deployment{deployment}, getDeleteDeployments(state, status, spec, true))
			if policy == api.WorkerRetirementExternal {
				status.CurrentVersion = &api.CurrentWorkerDeploymentVersion{BaseWorkerDeploymentVersion: api.BaseWorkerDeploymentVersion{BuildID: "B"}}
				require.Empty(t, getDeleteDeployments(state, status, spec, true), "routing beats an invalid retirement request")
				require.NotEqual(t, uint32(0), getScaleDeployments(state, status, spec)[ref])
				require.Empty(t, externalRetirementResources(state, status, spec))
				status.CurrentVersion = nil
				delete(state.Deployments, "B")
				require.Len(t, externalRetirementResources(state, status, spec), 1, "retry attached-resource removal after parent loss")
			}
		})
	}
}
