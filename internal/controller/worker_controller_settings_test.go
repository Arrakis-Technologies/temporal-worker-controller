// Unless explicitly stated otherwise all files in this repository are licensed under the MIT License.
//
// This product includes software developed at Datadog, Inc. Copyright 2024 Datadog, Inc.

package controller

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	api "github.com/temporalio/temporal-worker-controller/api/v1alpha1"
	"go.temporal.io/api/serviceerror"
	apps "k8s.io/api/apps/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/util/workqueue"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
	runtimecontroller "sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
)

type settingsQueueSource struct {
	requests []reconcile.Request
}

func (s settingsQueueSource) Start(_ context.Context, queue workqueue.TypedRateLimitingInterface[reconcile.Request]) error {
	for _, request := range s.requests {
		queue.Add(request)
	}
	return nil
}

func TestConfiguredConcurrencyBoundsActualReconciles(t *testing.T) {
	for _, tc := range []struct {
		name                       string
		configured, active, queued int
	}{
		{"HQ limit", 1, 1, 3},
		{"configured parallelism", 2, 2, 3},
		{"unchanged default", 0, 100, 101},
		{"direct caller fallback", -1, 100, 101},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			started := make(chan types.NamespacedName, tc.queued)
			release := make(chan struct{})
			r, _ := newTestReconcilerWithInterceptors(nil, interceptor.Funcs{
				Get: func(ctx context.Context, c client.WithWatch, key client.ObjectKey, object client.Object, opts ...client.GetOption) error {
					if _, ok := object.(*api.WorkerDeployment); ok {
						started <- key
						select {
						case <-release:
						case <-ctx.Done():
							return ctx.Err()
						}
					}
					return c.Get(ctx, key, object, opts...)
				},
			})
			r.MaxConcurrentReconciles = tc.configured
			recoverPanic := false
			options := r.controllerOptions(&recoverPanic)
			options.Reconciler = r
			skipNameValidation := true
			options.SkipNameValidation = &skipNameValidation
			controller, err := runtimecontroller.NewUnmanaged("settings-test", options)
			require.NoError(t, err)
			requests := make([]reconcile.Request, tc.queued)
			for i := range requests {
				requests[i].NamespacedName = types.NamespacedName{Namespace: "test", Name: fmt.Sprintf("worker-%d", i)}
			}
			require.NoError(t, controller.Watch(settingsQueueSource{requests: requests}))
			done := make(chan error, 1)
			go func() { done <- controller.Start(ctx) }()
			t.Cleanup(func() {
				cancel()
				select {
				case err := <-done:
					require.NoError(t, err)
				case <-time.After(5 * time.Second):
					t.Error("controller did not stop")
				}
			})
			for range tc.active {
				select {
				case <-started:
				case <-time.After(5 * time.Second):
					t.Fatal("configured worker did not start")
				}
			}
			select {
			case request := <-started:
				t.Fatalf("reconcile exceeded configured concurrency: %v", request)
			case <-time.After(150 * time.Millisecond):
			}
			close(release)
			for remaining := tc.active; remaining < tc.queued; remaining++ {
				select {
				case <-started:
				case <-time.After(5 * time.Second):
					t.Fatal("queued reconciliation did not proceed after the previous worker finished")
				}
			}
		})
	}
}

func TestSuccessfulReconcileUsesConfiguredInterval(t *testing.T) {
	for _, tc := range []struct {
		name           string
		interval, want time.Duration
	}{
		{"HQ interval", 5 * time.Second, 5 * time.Second},
		{"custom interval", 2 * time.Minute, 2 * time.Minute},
		{"unchanged default", 0, 10 * time.Second},
		{"direct caller fallback", -time.Second, 10 * time.Second},
	} {
		t.Run(tc.name, func(t *testing.T) {
			connection := makeNoCredsConnection("conn", "test", "localhost:7233")
			worker := makeWD("worker", "test", connection.Name)
			r, _ := newTestReconciler([]client.Object{worker, connection})
			r.ReconcileInterval = tc.interval
			r.TemporalClientPool.SetClientForTesting(noCredsPoolKey(connection.Spec.HostPort, testTemporalNamespace), newStubTemporalClient(nil))
			result, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: client.ObjectKeyFromObject(worker)})
			require.NoError(t, err)
			require.Equal(t, tc.want, result.RequeueAfter)
			var deployments apps.DeploymentList
			require.NoError(t, r.List(context.Background(), &deployments))
			require.Len(t, deployments.Items, 1, "successful path must create the requested worker")
		})
	}
}

func TestConfiguredIntervalPreservesRateLimitRetry(t *testing.T) {
	connection := makeNoCredsConnection("conn", "test", "localhost:7233")
	worker := makeWD("worker", "test", connection.Name)
	r, _ := newTestReconciler([]client.Object{worker, connection})
	r.ReconcileInterval = 5 * time.Second
	stub := newStubTemporalClient(nil)
	stub.describeDeploymentErr = &serviceerror.ResourceExhausted{}
	r.TemporalClientPool.SetClientForTesting(noCredsPoolKey(connection.Spec.HostPort, testTemporalNamespace), stub)
	result, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: client.ObjectKeyFromObject(worker)})
	require.NoError(t, err)
	require.Equal(t, 30*time.Second, result.RequeueAfter, "HQ poll interval must not override rate-limit backoff")
}

func TestConfiguredIntervalPreservesConflictRetry(t *testing.T) {
	connection := makeNoCredsConnection("conn", "test", "localhost:7233")
	worker := makeWD("worker", "test", connection.Name)
	r, _ := newTestReconcilerWithInterceptors([]client.Object{worker, connection}, interceptor.Funcs{
		SubResourceUpdate: func(ctx context.Context, c client.Client, subresource string, object client.Object, opts ...client.SubResourceUpdateOption) error {
			if _, ok := object.(*api.WorkerDeployment); ok && subresource == "status" {
				return apierrors.NewConflict(schema.GroupResource{Group: "temporal.io", Resource: "workerdeployments"}, object.GetName(), fmt.Errorf("changed concurrently"))
			}
			return c.SubResource(subresource).Update(ctx, object, opts...)
		},
	})
	r.ReconcileInterval = 5 * time.Second
	r.TemporalClientPool.SetClientForTesting(noCredsPoolKey(connection.Spec.HostPort, testTemporalNamespace), newStubTemporalClient(nil))
	result, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: client.ObjectKeyFromObject(worker)})
	require.NoError(t, err)
	require.Equal(t, time.Second, result.RequeueAfter, "HQ poll interval must not override the conflict retry")
}
