package v1alpha1

import (
	"context"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestExternalRetirementCannotBeDisabled(t *testing.T) {
	ctx := context.Background()
	old := &WorkerDeployment{}
	require.NoError(t, old.Spec.Default(ctx))
	require.Equal(t, WorkerRetirementAutomatic, old.Spec.SunsetStrategy.RetirementPolicy)
	old.Spec.SunsetStrategy.RetirementPolicy = WorkerRetirementExternal
	for _, policy := range []WorkerRetirementPolicy{"", WorkerRetirementAutomatic, "Unknown"} {
		next := old.DeepCopy()
		next.Spec.SunsetStrategy.RetirementPolicy = policy
		_, err := old.ValidateUpdate(ctx, old, next)
		require.Error(t, err)
	}
	next := old.DeepCopy()
	next.Spec.SunsetStrategy.Retirements = []WorkerVersionRetirement{{BuildID: "B", RequestID: "job"}}
	_, err := old.ValidateUpdate(ctx, old, next)
	require.NoError(t, err)
	next.Spec.SunsetStrategy.Retirements = append(next.Spec.SunsetStrategy.Retirements, WorkerVersionRetirement{BuildID: "B", RequestID: "other"})
	_, err = old.ValidateUpdate(ctx, old, next)
	require.Error(t, err, "two authorities for the same build must be rejected")
}
