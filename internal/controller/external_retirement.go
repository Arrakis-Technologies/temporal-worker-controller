package controller

import (
	"context"
	"fmt"

	api "github.com/temporalio/temporal-worker-controller/api/v1alpha1"
	"github.com/temporalio/temporal-worker-controller/internal/k8s"
	"github.com/temporalio/temporal-worker-controller/internal/planner"
	apps "k8s.io/api/apps/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// A fresh GET plus UID preconditions protects against replacement after the GET.
// Expected ownership and build identity also protect replacement before the GET.
func retirementResourceMatches(ref planner.WorkerResourceRef, object client.Object, workerName string) (bool, error) {
	if ref.BuildID == "" || ref.WRTUID == "" || ref.WRTName == "" {
		return false, fmt.Errorf("external retirement resource %s/%s lacks expected owner/build identity", ref.Namespace, ref.Name)
	}
	owner := meta.GetControllerOf(object)
	labels := k8s.ComputeSelectorLabels(workerName, ref.BuildID)
	return owner != nil && owner.UID == ref.WRTUID && owner.Name == ref.WRTName &&
		owner.APIVersion == api.GroupVersion.String() && owner.Kind == "WorkerResourceTemplate" &&
		object.GetLabels()[k8s.BuildIDLabel] == labels[k8s.BuildIDLabel] &&
		object.GetLabels()[k8s.WorkerDeploymentNameLabel] == labels[k8s.WorkerDeploymentNameLabel], nil
}

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
	poolLabel := k8s.ComputeSelectorLabels(worker.Name, "")[k8s.WorkerDeploymentNameLabel]
	if err := r.retirementReader().List(ctx, &deployments, client.InNamespace(worker.Namespace), client.MatchingLabels{k8s.WorkerDeploymentNameLabel: poolLabel}); err != nil {
		return err
	}
	for _, request := range worker.Spec.SunsetStrategy.Retirements {
		absent := true
		buildLabel := k8s.ComputeSelectorLabels(worker.Name, request.BuildID)[k8s.BuildIDLabel]
		for _, deployment := range deployments.Items {
			if deployment.Labels[k8s.BuildIDLabel] == buildLabel {
				absent = false
			}
		}
		for _, ref := range p.DeleteWorkerResources {
			if ref.BuildID != request.BuildID {
				continue
			}
			object := &unstructured.Unstructured{}
			gv, err := schema.ParseGroupVersion(ref.APIVersion)
			if err != nil {
				return err
			}
			object.SetGroupVersionKind(gv.WithKind(ref.Kind))
			err = r.retirementReader().Get(ctx, client.ObjectKey{Namespace: ref.Namespace, Name: ref.Name}, object)
			if err == nil {
				matches, err := retirementResourceMatches(ref, object, worker.Name)
				if err != nil {
					return err
				}
				if matches {
					absent = false
				}
			} else if !apierrors.IsNotFound(err) {
				return err
			}
		}
		// Routed/target requests are never acknowledged even if their creator
		// is missing. Only the external owner can repair/remove an invalid request.
		if request.RequestID == "" || planner.RetirementBlocked(&worker.Status, &worker.Spec, request.BuildID) {
			absent = false
		}
		if absent {
			status.RetiredVersions = append(status.RetiredVersions, request)
		}
	}
	worker.Status.Retirement = status
	return nil
}
