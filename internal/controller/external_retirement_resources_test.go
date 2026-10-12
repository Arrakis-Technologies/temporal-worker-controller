package controller

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/go-logr/logr"
	"github.com/stretchr/testify/require"
	api "github.com/temporalio/temporal-worker-controller/api/v1alpha1"
	"github.com/temporalio/temporal-worker-controller/internal/k8s"
	"github.com/temporalio/temporal-worker-controller/internal/planner"
	"github.com/temporalio/temporal-worker-controller/internal/temporal"
	apps "k8s.io/api/apps/v1"
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
)

// Use the real planner and renderer so deletion authority must survive their
// boundary; a hand-written deletion plan would miss lost owner/build identity.
func retirementResourceFixture(t *testing.T) (*api.WorkerDeployment, *plan, []*unstructured.Unstructured) {
	t.Helper()
	return retirementResourceFixtureFor(t, "pool", []string{"B", "C"})
}

func retirementResourceFixtureFor(t *testing.T, pool string, builds []string) (*api.WorkerDeployment, *plan, []*unstructured.Unstructured) {
	t.Helper()
	worker := &api.WorkerDeployment{ObjectMeta: meta.ObjectMeta{Name: pool, Namespace: "test", UID: "pool-uid", Generation: 2}}
	worker.Spec.SunsetStrategy = api.SunsetStrategy{RetirementPolicy: api.WorkerRetirementExternal, ScaledownDelay: &meta.Duration{}, DeleteDelay: &meta.Duration{}}
	worker.Status.TargetVersion.BuildID = "A"
	wrt := retirementTemplate(pool)
	var resources []*unstructured.Unstructured
	for _, build := range builds {
		worker.Spec.SunsetStrategy.Retirements = append(worker.Spec.SunsetStrategy.Retirements, api.WorkerVersionRetirement{BuildID: build, RequestID: "job-" + build})
		worker.Status.DeprecatedVersions = append(worker.Status.DeprecatedVersions, &api.DeprecatedWorkerDeploymentVersion{BaseWorkerDeploymentVersion: api.BaseWorkerDeploymentVersion{BuildID: build, Status: api.VersionStatusDrained}, DrainedSince: &meta.Time{Time: time.Now().Add(-time.Hour)}})
		resource, err := k8s.RenderWorkerResourceTemplate(&wrt, &apps.Deployment{ObjectMeta: meta.ObjectMeta{Name: "worker-" + build}}, build, "test")
		require.NoError(t, err)
		resource.SetUID(types.UID("resource-" + build))
		resources = append(resources, resource)
	}
	p, err := planner.GeneratePlan(logr.Discard(), &k8s.DeploymentState{Deployments: map[string]*apps.Deployment{}}, &worker.Status, &worker.Spec, nil, api.ConnectionSpec{}, &planner.Config{}, "test.pool", 75, nil, false, []api.WorkerResourceTemplate{wrt}, worker.Name, worker.UID)
	require.NoError(t, err)
	require.Len(t, p.DeleteWorkerResources, len(builds))
	return worker, &plan{DeleteWorkerResources: p.DeleteWorkerResources}, resources
}

func retirementTemplate(pool string) api.WorkerResourceTemplate {
	return api.WorkerResourceTemplate{
		ObjectMeta: meta.ObjectMeta{Name: "dependency", Namespace: "test", UID: "template-uid"},
		Spec:       api.WorkerResourceTemplateSpec{WorkerDeploymentRef: &api.WorkerDeploymentReference{Name: pool}, Template: runtime.RawExtension{Raw: []byte(`{"apiVersion":"v1","kind":"ConfigMap","data":{"config":"required"}}`)}},
	}
}

func TestExternalRetirementSkipsFenceReadsForUnchangedResources(t *testing.T) {
	worker, _, resources := retirementResourceFixture(t)
	wrt := retirementTemplate(worker.Name)
	reads := 0
	r, _ := newTestReconcilerWithInterceptors([]client.Object{worker, &wrt}, interceptor.Funcs{
		Get: func(ctx context.Context, c client.WithWatch, key client.ObjectKey, object client.Object, opts ...client.GetOption) error {
			if _, ok := object.(*api.WorkerDeployment); ok {
				reads++
			}
			return c.Get(ctx, key, object, opts...)
		},
	})
	p := &plan{WorkerDeploymentName: "test/pool", ApplyWorkerResources: []planner.WorkerResourceApply{
		{WRTName: wrt.Name, WRTNamespace: wrt.Namespace, BuildID: "B", RenderError: errors.New("invalid template")},
		{WRTName: wrt.Name, WRTNamespace: wrt.Namespace, BuildID: "C", Resource: resources[1], RenderedHash: "same", LastAppliedHash: "same"},
	}}
	require.Error(t, r.executePlan(context.Background(), logr.Discard(), worker, newStubTemporalClient(nil), p))
	require.Equal(t, 1, reads, "render failures and unchanged resources need no additional retirement reads")
}

func TestExternalRetirementMatchesRenderedLongIdentity(t *testing.T) {
	worker, p, resources := retirementResourceFixtureFor(t, strings.Repeat("pool", 20), []string{strings.Repeat("b", 70)})
	r, _ := newTestReconciler([]client.Object{worker, resources[0]})
	require.NoError(t, r.acknowledgeRetirement(context.Background(), worker, p))
	require.Empty(t, worker.Status.Retirement.RetiredVersions, "rendered label normalization must not hide an owned dependency")
	require.NoError(t, r.executeK8sOperations(context.Background(), logr.Discard(), worker, p))
	deployment := &apps.Deployment{ObjectMeta: meta.ObjectMeta{Name: "worker", Namespace: worker.Namespace, Labels: k8s.ComputeSelectorLabels(worker.Name, worker.Spec.SunsetStrategy.Retirements[0].BuildID)}}
	require.NoError(t, r.Create(context.Background(), deployment))
	require.NoError(t, r.acknowledgeRetirement(context.Background(), worker, p))
	require.Empty(t, worker.Status.Retirement.RetiredVersions, "normalized worker labels must still block retirement")
	require.NoError(t, r.Delete(context.Background(), deployment))
	require.NoError(t, r.acknowledgeRetirement(context.Background(), worker, p))
	require.Equal(t, worker.Spec.SunsetStrategy.Retirements, worker.Status.Retirement.RetiredVersions)
}

func TestExternalRetirementPreservesForeignResource(t *testing.T) {
	for _, change := range []string{"owner", "build", "pool", "unowned", "owned"} {
		t.Run(change, func(t *testing.T) {
			worker, p, resources := retirementResourceFixture(t)
			resource := resources[0]
			switch change {
			case "owner":
				owners := resource.GetOwnerReferences()
				owners[0].UID = "replacement-template"
				resource.SetOwnerReferences(owners)
			case "build":
				labels := resource.GetLabels()
				labels[k8s.BuildIDLabel] = "different-build"
				resource.SetLabels(labels)
			case "pool":
				labels := resource.GetLabels()
				labels[k8s.WorkerDeploymentNameLabel] = "different-pool"
				resource.SetLabels(labels)
			case "unowned":
				resource.SetOwnerReferences(nil)
			}
			r, _ := newTestReconciler([]client.Object{worker, resource})
			require.NoError(t, r.executeK8sOperations(context.Background(), logr.Discard(), worker, p))
			got := &unstructured.Unstructured{}
			got.SetGroupVersionKind(resource.GroupVersionKind())
			err := r.Get(context.Background(), client.ObjectKeyFromObject(resource), got)
			if change == "owned" {
				require.True(t, client.IgnoreNotFound(err) == nil && err != nil, "owned resource must be removed")
			} else {
				require.NoError(t, err, "same-name foreign replacement must survive")
				require.Equal(t, resource.GetUID(), got.GetUID())
			}
		})
	}
}

func TestExternalRetirementAcknowledgesBuildsIndependently(t *testing.T) {
	worker, p, resources := retirementResourceFixture(t)
	// B is already absent; C is held by a finalizer across reconciliation/restart.
	resources[1].SetFinalizers([]string{"test.example/hold"})
	r, _ := newTestReconciler([]client.Object{worker, resources[1]})
	require.NoError(t, r.executeK8sOperations(context.Background(), logr.Discard(), worker, p))
	require.NoError(t, r.acknowledgeRetirement(context.Background(), worker, p))
	require.Equal(t, worker.Spec.SunsetStrategy.Retirements[:1], worker.Status.Retirement.RetiredVersions)

	// A fresh reconciler must reach the same conclusion from persisted resources.
	r2 := &WorkerDeploymentReconciler{Client: r.Client, APIReader: r.Client}
	require.NoError(t, r2.acknowledgeRetirement(context.Background(), worker, p))
	require.Equal(t, worker.Spec.SunsetStrategy.Retirements[:1], worker.Status.Retirement.RetiredVersions)
	got := resources[1].DeepCopy()
	require.NoError(t, r.Get(context.Background(), client.ObjectKeyFromObject(got), got))
	got.SetFinalizers(nil)
	require.NoError(t, r.Update(context.Background(), got))
	require.NoError(t, r2.acknowledgeRetirement(context.Background(), worker, p))
	require.Equal(t, worker.Spec.SunsetStrategy.Retirements, worker.Status.Retirement.RetiredVersions)
}

func TestExternalRetirementDoesNotAcknowledgeDrainingBuild(t *testing.T) {
	now, earlier := time.Now(), time.Now().Add(-time.Hour)
	for _, tc := range []struct {
		name    string
		status  api.VersionStatus
		since   *time.Time
		blocked bool
	}{
		{"draining", api.VersionStatusDraining, nil, true},
		{"drained without timestamp", api.VersionStatusDrained, nil, true},
		{"scale-down grace", api.VersionStatusDrained, &now, true},
		{"drainage finished", api.VersionStatusDrained, &earlier, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			worker, _, resources := retirementResourceFixture(t)
			worker.Spec.SunsetStrategy.ScaledownDelay = &meta.Duration{Duration: time.Minute}
			state := &k8s.DeploymentState{Deployments: map[string]*apps.Deployment{}}
			temporalState := &temporal.TemporalWorkerState{CurrentBuildID: "A", Versions: map[string]*temporal.VersionInfo{
				"A": {Status: api.VersionStatusCurrent}, "B": {Status: tc.status, DrainedSince: tc.since},
			}}
			worker.Status = *newStateMapper(state, temporalState, "test/pool").mapToStatus("A")
			p, err := planner.GeneratePlan(logr.Discard(), state, &worker.Status, &worker.Spec, temporalState, api.ConnectionSpec{}, &planner.Config{}, "test/pool", 75, nil, false, []api.WorkerResourceTemplate{retirementTemplate(worker.Name)}, worker.Name, worker.UID)
			require.NoError(t, err)
			r, _ := newTestReconciler([]client.Object{worker, resources[0]})
			planned := &plan{DeleteWorkerResources: p.DeleteWorkerResources}
			require.NoError(t, r.executeK8sOperations(context.Background(), logr.Discard(), worker, planned))
			got := resources[0].DeepCopy()
			err = r.Get(context.Background(), client.ObjectKeyFromObject(got), got)
			if tc.blocked {
				require.NoError(t, err, "drainage dependencies must survive Deployment loss")
			} else {
				require.Error(t, err)
				require.NoError(t, client.IgnoreNotFound(err))
			}
			require.NoError(t, r.acknowledgeRetirement(context.Background(), worker, planned))
			if tc.blocked {
				require.Equal(t, worker.Spec.SunsetStrategy.Retirements[1:], worker.Status.Retirement.RetiredVersions)
			} else {
				require.Equal(t, worker.Spec.SunsetStrategy.Retirements, worker.Status.Retirement.RetiredVersions)
			}
		})
	}
}
