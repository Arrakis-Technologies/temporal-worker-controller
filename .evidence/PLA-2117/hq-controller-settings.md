# PLA-2117: preserve HQ controller settings

User scope, 2026-10-08: local fixes and push only. No re-canary.
The canonical [development contract](https://github.com/Arrakis-Technologies/arrakis-platform/blob/docs/pla-2117-runtime-publication/.evidence/PLA-2117/development-contract.md)
records REG-HQ-001 and the HQ failure and recovery.

## Red

On source `5af0e27a904f1b68d5af02fb53aa55f2c87f307c`, `TestHQStartupFlags`
built the actual `cmd/main.go` executable and passed the unchanged HQ arguments.
It failed with exit 2: `flag provided but not defined: -max-concurrent-reconciles`.
The executable built successfully. This was the startup regression, not a test setup failure.

## Fix

Port the two runtime controls from HQ's released source `36b1164`, without replacing other controller behavior.
The executable accepts and validates `--max-concurrent-reconciles` and `--reconcile-interval`,
then passes both settings to the WorkerDeployment reconciler.
Positive settings configure the real controller queue and successful reconciliation delay.
Omitted settings retain concurrency 100 and interval 10s.
Direct callers with non-positive fields retain those defaults. Invalid CLI values fail before manager startup.
The validation diagnostic uses stderr because the structured logger is not initialized at that point.

HQ can keep its existing arguments: concurrency 1 and interval 5s.
Namespace-scoped caching, panic recovery, external retirement and special retry paths remain unchanged.
No CRD, Helm, SDK, database or deployment-manifest change is required.

## Local green

The startup regression passes against the real executable. Valid settings reach
configuration loading with an explicit missing kubeconfig, so the test cannot contact a cluster.
Zero, negative and malformed settings fail with a diagnostic.

A real controller-runtime queue executes the production reconciler with blocked test reads.
It allows exactly 1 or 2 simultaneous reconciliations when configured accordingly.
Omitted and negative direct-call settings allow 100, but keep the 101st queued until capacity becomes available.
Successful reconciliation creates the requested worker and returns the configured delay.
Conflict retries remain 1s. Rate-limit retries remain 30s.

Commands passed:

```sh
go test ./cmd ./internal/controller ./internal/controller/clientpool ./internal/planner ./internal/k8s ./internal/temporal -skip '^TestControllers$' -count=1
go test -race ./cmd ./internal/controller -run 'Test(HQStartupFlags|ConfiguredConcurrencyBoundsActualReconciles|SuccessfulReconcileUsesConfiguredInterval|ConfiguredIntervalPreserves(RateLimit|Conflict)Retry|ExternalRetirementStalePlanAndAcknowledgement)$' -count=5
go vet ./cmd ./internal/controller ./internal/controller/clientpool ./internal/planner ./internal/k8s ./internal/temporal
```

All commands used the temporary Nix Go toolchain (Go 1.26.7).
Formatting and `git diff --check` passed.
The envtest-backed `TestControllers` suite was excluded. No Kind lifecycle or HQ canary was run.
No image was built or published. The older canary image does not contain this fix.
