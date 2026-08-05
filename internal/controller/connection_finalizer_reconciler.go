// Unless explicitly stated otherwise all files in this repository are licensed under the MIT License.
//
// This product includes software developed at Datadog (https://www.datadoghq.com/). Copyright 2024 Datadog, Inc.

package controller

import (
	"context"

	temporaliov1alpha1 "github.com/temporalio/temporal-worker-controller/api/v1alpha1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
)

// ConnectionFinalizerReconciler closes the deletion race where the last
// WorkerDeployment disappears before its reconcile can release the Connection.
// The Connection finalizer remains fail-closed while either the current or the
// deprecated WorkerDeployment kind still references it.
type ConnectionFinalizerReconciler struct {
	client.Client
}

func (r *ConnectionFinalizerReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	var connection temporaliov1alpha1.Connection
	if err := r.Get(ctx, req.NamespacedName, &connection); err != nil {
		if apierrors.IsNotFound(err) {
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, err
	}

	if connection.DeletionTimestamp.IsZero() || !controllerutil.ContainsFinalizer(&connection, finalizerName) {
		return ctrl.Result{}, nil
	}

	referenced, err := r.connectionIsReferenced(ctx, &connection)
	if err != nil {
		return ctrl.Result{}, err
	}
	if referenced {
		return ctrl.Result{}, nil
	}

	controllerutil.RemoveFinalizer(&connection, finalizerName)
	if err := r.Update(ctx, &connection); err != nil {
		return ctrl.Result{}, err
	}
	return ctrl.Result{}, nil
}

func (r *ConnectionFinalizerReconciler) connectionIsReferenced(
	ctx context.Context,
	connection *temporaliov1alpha1.Connection,
) (bool, error) {
	var current temporaliov1alpha1.WorkerDeploymentList
	if err := r.List(ctx, &current, client.InNamespace(connection.Namespace)); err != nil {
		return false, err
	}
	for i := range current.Items {
		if current.Items[i].Spec.WorkerOptions.ConnectionRef.Name == connection.Name {
			return true, nil
		}
	}

	var deprecated temporaliov1alpha1.TemporalWorkerDeploymentList
	if err := r.List(ctx, &deprecated, client.InNamespace(connection.Namespace)); err != nil {
		return false, err
	}
	for i := range deprecated.Items {
		if deprecated.Items[i].Spec.WorkerOptions.TemporalConnectionRef.Name == connection.Name {
			return true, nil
		}
	}
	return false, nil
}

func connectionRequestForWorkerDeployment(_ context.Context, object client.Object) []reconcile.Request {
	var connectionName string
	switch workerDeployment := object.(type) {
	case *temporaliov1alpha1.WorkerDeployment:
		connectionName = workerDeployment.Spec.WorkerOptions.ConnectionRef.Name
	case *temporaliov1alpha1.TemporalWorkerDeployment:
		connectionName = workerDeployment.Spec.WorkerOptions.TemporalConnectionRef.Name
	default:
		return nil
	}
	if connectionName == "" {
		return nil
	}
	return []reconcile.Request{{NamespacedName: types.NamespacedName{
		Name:      connectionName,
		Namespace: object.GetNamespace(),
	}}}
}

func (r *ConnectionFinalizerReconciler) SetupWithManager(manager ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(manager).
		For(&temporaliov1alpha1.Connection{}).
		Watches(
			&temporaliov1alpha1.WorkerDeployment{},
			handler.EnqueueRequestsFromMapFunc(connectionRequestForWorkerDeployment),
		).
		Watches(
			&temporaliov1alpha1.TemporalWorkerDeployment{},
			handler.EnqueueRequestsFromMapFunc(connectionRequestForWorkerDeployment),
		).
		Named("connection-finalizer").
		Complete(r)
}
