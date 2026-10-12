# Remaining CodeRabbit findings — 2026-10-12

User authorization: "either fix or resolve these". Starting source: `6f2bf37a`.
Canonical contract: platform PR #852, `.evidence/PLA-2117/development-contract.md`.

- Deprecated references: extend the existing retention and mapper tests to both worker kinds.
- Connection concurrency: keep Update's existing resource-version check. A regression injects
  a concurrent Connection write before finalizer removal. The stale update returns Conflict,
  preserving the finalizer and concurrent annotation. The proposed optimistic patch does not
  make worker creation and Connection deletion atomic across separate objects.
- External scaling: refresh the live resource version only for the planned Deployment UID.
  Reject replacement UIDs and deletion. Preserve scale preconditions against later mutations.

The existing API-server ownership suite reproduced a real scale Conflict after a metadata
update between planning and execution. It now proves successful same-UID restoration,
rejection of a second write after the fresh read, and rejection of a same-name replacement.
An initial test type error and canceled cleanup context were fixture corrections, not product evidence.

Command, under `nix shell nixpkgs#go nixpkgs#kubernetes-helm --command`:

`env KUBEBUILDER_ASSETS=bin/k8s/1.29.0-darwin-arm64 go test ./internal/planner
./internal/controller ./internal/k8s ./internal/temporal ./cmd ./api/v1alpha1 -count=1`

All six packages pass, including the real API-server ownership suite and webhook suite.
No public field, protocol, default retention behavior or SDK contract changes.
No image, deployment or current HQ proof is claimed.
