// Unless explicitly stated otherwise all files in this repository are licensed under the MIT License.

package controller

import (
	"context"
	"testing"
	"time"

	temporaliov1alpha1 "github.com/temporalio/temporal-worker-controller/api/v1alpha1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

func deletingConnection(name string) *temporaliov1alpha1.Connection {
	deletedAt := metav1.NewTime(time.Now())
	return &temporaliov1alpha1.Connection{
		ObjectMeta: metav1.ObjectMeta{
			Name:              name,
			Namespace:         "tenant-main",
			Finalizers:        []string{finalizerName},
			DeletionTimestamp: &deletedAt,
		},
	}
}

func workerDeployment(name, connectionName string) *temporaliov1alpha1.WorkerDeployment {
	return &temporaliov1alpha1.WorkerDeployment{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "tenant-main"},
		Spec: temporaliov1alpha1.WorkerDeploymentSpec{
			WorkerOptions: temporaliov1alpha1.WorkerOptions{
				ConnectionRef: temporaliov1alpha1.ConnectionReference{Name: connectionName},
			},
		},
	}
}

func deprecatedWorkerDeployment(name, connectionName string) *temporaliov1alpha1.TemporalWorkerDeployment {
	return &temporaliov1alpha1.TemporalWorkerDeployment{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "tenant-main"},
		Spec: temporaliov1alpha1.TemporalWorkerDeploymentSpec{
			WorkerOptions: temporaliov1alpha1.DeprecatedWorkerOptions{
				TemporalConnectionRef: temporaliov1alpha1.TemporalConnectionReference{Name: connectionName},
			},
		},
	}
}

func connectionFinalizerTestClient(t *testing.T, objects ...client.Object) client.Client {
	t.Helper()
	scheme := runtime.NewScheme()
	if err := temporaliov1alpha1.AddToScheme(scheme); err != nil {
		t.Fatalf("register scheme: %v", err)
	}
	return fake.NewClientBuilder().WithScheme(scheme).WithObjects(objects...).Build()
}

func TestConnectionFinalizerReconcilerRemovesOrphanedFinalizer(t *testing.T) {
	connection := deletingConnection("pool-connection")
	k8sClient := connectionFinalizerTestClient(t, connection)
	reconciler := &ConnectionFinalizerReconciler{Client: k8sClient}

	if _, err := reconciler.Reconcile(context.Background(), ctrl.Request{NamespacedName: types.NamespacedName{
		Name: connection.Name, Namespace: connection.Namespace,
	}}); err != nil {
		t.Fatalf("reconcile orphaned connection: %v", err)
	}

	var observed temporaliov1alpha1.Connection
	err := k8sClient.Get(context.Background(), client.ObjectKeyFromObject(connection), &observed)
	if err != nil && !apierrors.IsNotFound(err) {
		t.Fatalf("get orphaned connection after reconciliation: %v", err)
	}
	if err == nil && containsString(observed.Finalizers, finalizerName) {
		t.Fatalf("orphaned connection still has %q finalizer", finalizerName)
	}
}

func TestConnectionFinalizerReconcilerRetainsReferencedConnection(t *testing.T) {
	connection := deletingConnection("pool-connection")
	worker := workerDeployment("pool-worker", connection.Name)
	k8sClient := connectionFinalizerTestClient(t, connection, worker)
	reconciler := &ConnectionFinalizerReconciler{Client: k8sClient}

	if _, err := reconciler.Reconcile(context.Background(), ctrl.Request{NamespacedName: types.NamespacedName{
		Name: connection.Name, Namespace: connection.Namespace,
	}}); err != nil {
		t.Fatalf("reconcile referenced connection: %v", err)
	}

	var observed temporaliov1alpha1.Connection
	if err := k8sClient.Get(context.Background(), client.ObjectKeyFromObject(connection), &observed); err != nil {
		t.Fatalf("get referenced connection: %v", err)
	}
	if !containsString(observed.Finalizers, finalizerName) {
		t.Fatalf("referenced connection lost %q finalizer", finalizerName)
	}
}

func TestConnectionFinalizerReconcilerRetainsConnectionReferencedByDeprecatedWorker(t *testing.T) {
	connection := deletingConnection("pool-connection")
	worker := deprecatedWorkerDeployment("pool-worker", connection.Name)
	k8sClient := connectionFinalizerTestClient(t, connection, worker)
	reconciler := &ConnectionFinalizerReconciler{Client: k8sClient}

	if _, err := reconciler.Reconcile(context.Background(), ctrl.Request{NamespacedName: types.NamespacedName{
		Name: connection.Name, Namespace: connection.Namespace,
	}}); err != nil {
		t.Fatalf("reconcile connection referenced by deprecated worker: %v", err)
	}

	var observed temporaliov1alpha1.Connection
	if err := k8sClient.Get(context.Background(), client.ObjectKeyFromObject(connection), &observed); err != nil {
		t.Fatalf("get connection referenced by deprecated worker: %v", err)
	}
	if !containsString(observed.Finalizers, finalizerName) {
		t.Fatalf("connection referenced by deprecated worker lost %q finalizer", finalizerName)
	}
}

func TestConnectionRequestForWorkerDeploymentUsesExactReference(t *testing.T) {
	worker := workerDeployment("pool-worker", "pool-connection")
	requests := connectionRequestForWorkerDeployment(context.Background(), worker)
	if len(requests) != 1 {
		t.Fatalf("request count = %d, want 1", len(requests))
	}
	if got := requests[0].NamespacedName; got.Name != "pool-connection" || got.Namespace != "tenant-main" {
		t.Fatalf("request = %v, want tenant-main/pool-connection", got)
	}
}

func TestConnectionRequestForDeprecatedWorkerDeploymentUsesExactReference(t *testing.T) {
	worker := deprecatedWorkerDeployment("pool-worker", "pool-connection")
	requests := connectionRequestForWorkerDeployment(context.Background(), worker)
	if len(requests) != 1 {
		t.Fatalf("request count = %d, want 1", len(requests))
	}
	if got := requests[0].NamespacedName; got.Name != "pool-connection" || got.Namespace != "tenant-main" {
		t.Fatalf("request = %v, want tenant-main/pool-connection", got)
	}
}

func containsString(items []string, expected string) bool {
	for _, item := range items {
		if item == expected {
			return true
		}
	}
	return false
}
