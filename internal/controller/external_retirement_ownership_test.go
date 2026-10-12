package controller

import (
	"context"
	"encoding/json"

	"github.com/go-logr/logr"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	api "github.com/temporalio/temporal-worker-controller/api/v1alpha1"
	"github.com/temporalio/temporal-worker-controller/internal/k8s"
	apps "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
)

var _ = Describe("External retirement field ownership", func() {
	It("refreshes a retained worker's scale fence without adopting a replacement", func(ctx SpecContext) {
		namespace := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{GenerateName: "retirement-scale-"}}
		Expect(k8sClient.Create(ctx, namespace)).To(Succeed())
		DeferCleanup(func(ctx context.Context) { Expect(k8sClient.Delete(ctx, namespace)).To(Succeed()) })
		worker := makeWD("pool", namespace.Name, "connection")
		worker.Spec.SunsetStrategy.RetirementPolicy = api.WorkerRetirementExternal
		Expect(k8sClient.Create(ctx, worker)).To(Succeed())
		deployment := &apps.Deployment{
			ObjectMeta: metav1.ObjectMeta{Name: "old-worker", Namespace: namespace.Name},
			Spec: apps.DeploymentSpec{
				Replicas: new(int32(0)), Selector: &metav1.LabelSelector{MatchLabels: map[string]string{"app": "worker"}},
				Template: worker.Spec.Template,
			},
		}
		deployment.Spec.Template.Labels = map[string]string{"app": "worker"}
		Expect(k8sClient.Create(ctx, deployment)).To(Succeed())
		controller, _ := newTestReconciler(nil)
		controller.Client, controller.APIReader = k8sClient, k8sClient
		scalePlan := func() *plan {
			return &plan{ScaleDeployments: map[*corev1.ObjectReference]uint32{k8s.NewObjectRef(deployment): 1}}
		}
		stale := scalePlan()
		deployment.Annotations = map[string]string{"test": "changed-after-cache"}
		Expect(k8sClient.Update(ctx, deployment)).To(Succeed())
		Expect(controller.executeK8sOperations(ctx, logr.Discard(), worker, stale)).To(Succeed())
		Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(deployment), deployment)).To(Succeed())
		Expect(*deployment.Spec.Replicas).To(Equal(int32(1)))

		By("rejecting a second change between the fresh read and scale")
		deployment.Spec.Replicas = new(int32(0))
		Expect(k8sClient.Update(ctx, deployment)).To(Succeed())
		watchClient, err := client.NewWithWatch(cfg, client.Options{Scheme: k8sClient.Scheme()})
		Expect(err).NotTo(HaveOccurred())
		controller.APIReader = interceptor.NewClient(watchClient, interceptor.Funcs{
			Get: func(ctx context.Context, c client.WithWatch, key client.ObjectKey, object client.Object, opts ...client.GetOption) error {
				if err := c.Get(ctx, key, object, opts...); err != nil {
					return err
				}
				if current, ok := object.(*apps.Deployment); ok {
					changed := current.DeepCopy()
					changed.Annotations["test"] = "changed-after-fresh-read"
					return c.Update(ctx, changed)
				}
				return nil
			},
		})
		Expect(apierrors.IsConflict(controller.executeK8sOperations(ctx, logr.Discard(), worker, scalePlan()))).To(BeTrue())
		Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(deployment), deployment)).To(Succeed())
		Expect(*deployment.Spec.Replicas).To(Equal(int32(0)))

		By("rejecting a same-name replacement")
		controller.APIReader = k8sClient
		stale = scalePlan()
		Expect(k8sClient.Delete(ctx, deployment)).To(Succeed())
		deployment.UID, deployment.ResourceVersion = "", ""
		Expect(k8sClient.Create(ctx, deployment)).To(Succeed())
		Expect(apierrors.IsConflict(controller.executeK8sOperations(ctx, logr.Discard(), worker, stale))).To(BeTrue())
		Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(deployment), deployment)).To(Succeed())
		Expect(*deployment.Spec.Replicas).To(Equal(int32(0)))
	})

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
