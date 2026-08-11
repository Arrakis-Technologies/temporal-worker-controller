# PLA-1882 — Controller vulnerability remediation

## Current runtime fact

HQ runs the recovery controller image built from commit
`0236e5e7780a1ce7c76740ffab597ee41a0523ad`, not from current `main`. That
recovery branch preserves bounded reconciliation and orphaned Connection
cleanup, but still resolves `golang.org/x/net v0.48.0` and other vulnerable
dependencies.

The public chart `0.26.0` used by eu-shared-1 and Scorpio selects controller
`1.7.0`, whose root module also resolves `golang.org/x/net v0.48.0`.

## Root causes covered

- `golang.org/x/net < 0.55.0`: CVE-2026-39821 / Vanta AWS critical.
- `google.golang.org/grpc < 1.82.1`: GitHub alerts 2 and 3; reachable according
  to `govulncheck` before the update.
- `github.com/apache/thrift < 0.23.0`: GitHub alert 1.
- `golang.org/x/text < 0.39.0` and OpenTelemetry < 1.44.0: additional package
  vulnerabilities found during the focused `govulncheck` pass.

## Platform Quality Packet

- **Invariant:** The exact recovery-controller source keeps both Arrakis
  recovery fixes while all three Go modules resolve the fixed dependency
  versions.
- **Runtime path:** recovery branch -> Arrakis platform release workflow ->
  immutable ECR image -> arrakis-deploy image pin -> Argo CD -> controller pod.
- **Affected substrate:** Controller dependency graph only. No CRD, API,
  reconciliation, storage, authorization, or Helm behavior changes.
- **Entrypoints:** Controller binary, demo worker, and integration-test module.
- **Lifecycle:** Module download, compilation, controller startup, HQ canary,
  rollback to the prior immutable recovery image.
- **Adversarial cases:** Only current main is patched while the recovery image
  remains stale; one nested Go module retains a vulnerable version; the new
  graph fails to compile; GitOps continues to pin the old image.
- **Non-goals:** Publishing or deploying an image in this source PR; merging
  the separate recovery-feature PR; changing controller behavior.
- **Rollback:** Re-pin the previous immutable recovery image. Do not revert the
  source security patch merely to roll back a runtime deployment.

## Local validation — 2026-08-11

- Root `go mod verify`: pass.
- Root `go test ./...`: pass.
- Demo `go mod verify` and `go test ./...`: pass.
- Root `govulncheck ./...`: zero affected symbols and zero vulnerable imported
  packages after the update.
- Version inspection: root, demo, and integration-test modules use x/net
  0.56.0, x/text 0.39.0, gRPC 1.82.1, and OpenTelemetry 1.44.0; root and tests
  use Thrift 0.23.0.
- Integration-test compile-only gate: baseline failure reproduced on untouched
  v1.7.0 as well as this branch. Its released controller v1.0.1 dependency uses
  `BuildId` against an SDK exposing `BuildID`; this PR does not introduce that
  existing incompatibility.

## Proof boundary

Local checks prove the dependency-only source patch. Hosted CI, review, an
immutable image build, image scanning, HQ runtime health, wider GitOps
promotion, AWS Inspector, GitHub, and Vanta convergence remain separate gates.
