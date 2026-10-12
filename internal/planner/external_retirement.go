package planner

import (
	"time"

	api "github.com/temporalio/temporal-worker-controller/api/v1alpha1"
	"github.com/temporalio/temporal-worker-controller/internal/k8s"
	apps "k8s.io/api/apps/v1"
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func externalRetirement(spec *api.WorkerDeploymentSpec) bool {
	return spec.SunsetStrategy.RetirementPolicy == api.WorkerRetirementExternal
}

func externallyRetained(spec *api.WorkerDeploymentSpec, build string) bool {
	if !externalRetirement(spec) {
		return false
	}
	for _, request := range spec.SunsetStrategy.Retirements {
		if request.BuildID == build && request.RequestID != "" {
			return false
		}
	}
	return true
}

func retirementRouted(status *api.WorkerDeploymentStatus, build string) bool {
	if status.TargetVersion.BuildID == build || (status.CurrentVersion != nil && status.CurrentVersion.BuildID == build) {
		return true
	}
	for _, version := range status.DeprecatedVersions {
		if version.BuildID == build && (version.Status == api.VersionStatusCurrent || version.Status == api.VersionStatusRamping) {
			return true
		}
	}
	return false
}

// RetirementBlocked also protects attached execution dependencies and ACKs when
// a worker Deployment disappears before Temporal finishes draining its workflows.
func RetirementBlocked(status *api.WorkerDeploymentStatus, spec *api.WorkerDeploymentSpec, build string) bool {
	if retirementRouted(status, build) {
		return true
	}
	for _, version := range status.DeprecatedVersions {
		if version.BuildID != build {
			continue
		}
		if version.Status == api.VersionStatusDraining {
			return true
		}
		if version.Status == api.VersionStatusDrained {
			return version.DrainedSince == nil || (spec.SunsetStrategy.ScaledownDelay != nil &&
				time.Since(version.DrainedSince.Time) <= spec.SunsetStrategy.ScaledownDelay.Duration)
		}
	}
	return false
}

// Keep every attachment during drainage, then remove it before scaling down.
// Retry after Deployment loss: synthetic targets identify attachments only.
func externalRetirementResources(state *k8s.DeploymentState, status *api.WorkerDeploymentStatus, spec *api.WorkerDeploymentSpec) []*apps.Deployment {
	if !externalRetirement(spec) {
		return nil
	}
	var resources []*apps.Deployment
	for _, request := range spec.SunsetStrategy.Retirements {
		if request.RequestID == "" || RetirementBlocked(status, spec, request.BuildID) {
			continue
		}
		if deployment, present := state.Deployments[request.BuildID]; present {
			resources = append(resources, deployment)
		} else {
			resources = append(resources, &apps.Deployment{ObjectMeta: meta.ObjectMeta{Labels: map[string]string{k8s.BuildIDLabel: request.BuildID}}})
		}
	}
	return resources
}
