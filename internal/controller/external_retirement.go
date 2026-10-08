package controller

import (
	"context"
	"fmt"

	api "github.com/temporalio/temporal-worker-controller/api/v1alpha1"
	"github.com/temporalio/temporal-worker-controller/internal/k8s"
	apps "k8s.io/api/apps/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

func (r *WorkerDeploymentReconciler) retirementReader() client.Reader {
	if r.APIReader != nil {
		return r.APIReader
	}
	return r.Client // fake-client tests; production always supplies an uncached reader
}

// A stale release plan must not survive withdrawal of the exact request or a
// changed template. Concrete Deployment UID/resourceVersion preconditions also
// prevent it from acting on a same-name build recreated after fence completion.
func (r *WorkerDeploymentReconciler) checkRetirementPlan(ctx context.Context, planned *api.WorkerDeployment) error {
	var current api.WorkerDeployment
	if err := r.retirementReader().Get(ctx, client.ObjectKeyFromObject(planned), &current); err != nil {
		if apierrors.IsNotFound(err) && planned.Spec.SunsetStrategy.RetirementPolicy != api.WorkerRetirementExternal {
			return nil // preserve legacy automatic-sunset semantics
		}
		return err
	}
	if current.Spec.SunsetStrategy.RetirementPolicy != api.WorkerRetirementExternal && planned.Spec.SunsetStrategy.RetirementPolicy != api.WorkerRetirementExternal {
		return nil
	}
	// An Automatic plan made before enrollment must also be invalidated.
	if current.UID != planned.UID || current.Generation != planned.Generation || current.DeletionTimestamp != nil {
		return apierrors.NewConflict(schema.GroupResource{Group: "temporal.io", Resource: "workerdeployments"}, planned.Name, fmt.Errorf("external retirement plan became stale"))
	}
	if current.Spec.SunsetStrategy.RetirementPolicy != planned.Spec.SunsetStrategy.RetirementPolicy {
		return apierrors.NewConflict(schema.GroupResource{Group: "temporal.io", Resource: "workerdeployments"}, planned.Name, fmt.Errorf("worker retirement authority changed"))
	}
	return nil
}

func (r *WorkerDeploymentReconciler) acknowledgeRetirement(ctx context.Context, worker *api.WorkerDeployment, p *plan) error {
	if worker.Spec.SunsetStrategy.RetirementPolicy != api.WorkerRetirementExternal {
		return nil
	}
	if err := r.checkRetirementPlan(ctx, worker); err != nil {
		return err
	}
	status := &api.WorkerRetirementStatus{Policy: api.WorkerRetirementExternal, ObservedGeneration: worker.Generation}
	var deployments apps.DeploymentList
	if err := r.retirementReader().List(ctx, &deployments, client.InNamespace(worker.Namespace), client.MatchingLabels{k8s.WorkerDeploymentNameLabel: worker.Name}); err != nil {
		return err
	}
	for _, request := range worker.Spec.SunsetStrategy.Retirements {
		absent := true
		for _, deployment := range deployments.Items {
			if deployment.Labels[k8s.BuildIDLabel] == request.BuildID {
				absent = false
			}
		}
		for _, ref := range p.DeleteWorkerResources {
			object := &unstructured.Unstructured{}
			gv, err := schema.ParseGroupVersion(ref.APIVersion)
			if err != nil {
				return err
			}
			object.SetGroupVersionKind(gv.WithKind(ref.Kind))
			err = r.retirementReader().Get(ctx, client.ObjectKey{Namespace: ref.Namespace, Name: ref.Name}, object)
			if err == nil {
				absent = false
			} else if !apierrors.IsNotFound(err) {
				return err
			}
		}
		// Routed/target requests are never acknowledged even if their creator
		// is missing. Only the external owner can repair/remove an invalid request.
		if worker.Status.TargetVersion.BuildID == request.BuildID ||
			(worker.Status.CurrentVersion != nil && worker.Status.CurrentVersion.BuildID == request.BuildID) {
			absent = false
		}
		for _, version := range worker.Status.DeprecatedVersions {
			if version.BuildID == request.BuildID && (version.Status == api.VersionStatusCurrent || version.Status == api.VersionStatusRamping) {
				absent = false
			}
		}
		if absent {
			status.RetiredVersions = append(status.RetiredVersions, request)
		}
	}
	worker.Status.Retirement = status
	return nil
}
