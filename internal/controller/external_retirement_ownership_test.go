package controller

import (
	"context"
	"encoding/json"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	api "github.com/temporalio/temporal-worker-controller/api/v1alpha1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

var _ = Describe("External retirement field ownership", func() {
	It("preserves permission and template updates while rejecting stale writers", func(ctx SpecContext) {
		namespace := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{GenerateName: "retirement-ownership-"}}
		Expect(k8sClient.Create(ctx, namespace)).To(Succeed())
		DeferCleanup(func(ctx context.Context) {
			Expect(k8sClient.Delete(ctx, namespace)).To(Succeed())
		})

		desired := makeWD("pool", namespace.Name, "connection")
		desired.Spec.SunsetStrategy.RetirementPolicy = api.WorkerRetirementExternal
		key := client.ObjectKeyFromObject(desired)
		// The Action operator owns the template and policy, but never applies requests.
		apply := func(image string) *api.WorkerDeployment {
			worker := desired.DeepCopy()
			worker.Spec.Template.Spec.Containers[0].Image = image
			Expect(k8sClient.Patch(ctx, worker, client.Apply,
				client.FieldOwner("action-operator-managed-runtime"), client.ForceOwnership)).To(Succeed())
			return worker
		}
		current := apply("temporal/worker:v1")
		DeferCleanup(func(ctx context.Context) {
			Expect(k8sClient.Delete(ctx, current)).To(Succeed())
		})
		patchRequests := func(worker *api.WorkerDeployment, requests []api.WorkerVersionRetirement) error {
			body, err := json.Marshal(map[string]any{
				"metadata": map[string]any{"resourceVersion": worker.ResourceVersion},
				"spec":     map[string]any{"sunset": map[string]any{"retirements": requests}},
			})
			Expect(err).NotTo(HaveOccurred())
			return k8sClient.Patch(ctx, worker, client.RawPatch(types.MergePatchType, body))
		}
		other := api.WorkerVersionRetirement{BuildID: "B", RequestID: "other-job"}
		own := api.WorkerVersionRetirement{BuildID: "C", RequestID: "cleanup-job"}
		Expect(patchRequests(current, []api.WorkerVersionRetirement{other})).To(Succeed())
		stale := current.DeepCopy()

		By("applying a changed template without removing another controller's request")
		current = apply("temporal/worker:v2")
		Expect(current.Spec.SunsetStrategy.Retirements).To(Equal([]api.WorkerVersionRetirement{other}))
		Expect(current.Generation).To(BeNumerically(">", stale.Generation))
		controller := &WorkerDeploymentReconciler{Client: k8sClient, APIReader: k8sClient}
		Expect(apierrors.IsConflict(controller.checkRetirementPlan(ctx, stale))).To(BeTrue())

		By("refusing a stale permission update and retrying from the current object")
		Expect(apierrors.IsConflict(patchRequests(stale, []api.WorkerVersionRetirement{other, own}))).To(BeTrue())
		Expect(k8sClient.Get(ctx, key, current)).To(Succeed())
		Expect(patchRequests(current, []api.WorkerVersionRetirement{other, own})).To(Succeed())
		Expect(current.Spec.Template.Spec.Containers[0].Image).To(Equal("temporal/worker:v2"))

		By("preserving both requests through another Action operator reconciliation")
		current = apply("temporal/worker:v3")
		Expect(current.Spec.SunsetStrategy.Retirements).To(Equal([]api.WorkerVersionRetirement{other, own}))
		Expect(controller.checkRetirementPlan(ctx, current)).To(Succeed())

		By("clearing only the completed job's permission without losing the template update")
		Expect(patchRequests(current, []api.WorkerVersionRetirement{other})).To(Succeed())
		Expect(k8sClient.Get(ctx, key, current)).To(Succeed())
		Expect(current.Spec.SunsetStrategy.Retirements).To(Equal([]api.WorkerVersionRetirement{other}))
		Expect(current.Spec.Template.Spec.Containers[0].Image).To(Equal("temporal/worker:v3"))
	})
})
