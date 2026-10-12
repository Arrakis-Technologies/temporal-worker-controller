# Retirement observation corrections — 2026-10-12

The user approved fixing drainage after Deployment loss, ambiguous Temporal observations,
and unnecessary no-op reads, then requested rebase, verification and push.
Starting controller source: `df58e6147404066eb2337781c8feecdd115e938a`.
The canonical platform contract remains in platform PR #852.

Use one complete observed status for planner removal and acknowledgement. Include Temporal
versions even when Kubernetes has no corresponding Deployment. Preserve drainage timestamps
and the existing grace period. Explicit Temporal NotFound remains confirmed absence. A successful
response without deployment or routing information becomes an error, so reconciliation retries
before planning destructive work. Existing RPC errors already stop reconciliation.

No public field or retention policy changes. Move the uncached retirement check below render-error
and unchanged-hash skips, keeping it before actual resource writes.

Before production edits, existing controller tests reproduced removal of attached ConfigMaps
for missing Deployments during drainage, missing timestamps and scale-down grace. Both
incomplete Temporal response contrasts returned success instead of error. Confirmed NotFound,
valid empty inventory, RPC failure and completed drainage controls behaved as expected.

Command: `go test ./internal/controller -run
'TestExternalRetirement(DoesNotAcknowledgeDrainingBuild|RequiresAuthoritativeTemporalObservation)' -count=1`
under `nix shell nixpkgs#go --command`.

The no-op regression reproduced three uncached WorkerDeployment reads where one was sufficient.
A missing Resource in its first fixture caused a test setup panic, corrected before recording red.
The fixed mapper includes both inventories. The reader rejects incomplete responses, and the
executor checks its fence only before actual applies. The affected planner, controller, Kubernetes,
Temporal, command and webhook packages pass. The webhook run required Helm in the test shell;
its first attempt failed in setup because Helm was absent, then passed with Helm present.
