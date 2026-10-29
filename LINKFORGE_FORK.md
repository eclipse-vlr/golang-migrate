# LinkForge resource cleanup patch

This fork preserves the upstream module path and MIT license. The patch branch is based on upstream v4.20.1; LinkForge will pin its exact revision through a Go module replacement.

The change closes and joins the current and prefetched migration buffers when execution stops early. It closes each source body once, interrupts blocked reads, and drains queued migrations without executing their SQL. Primary migration errors and dirty-version semantics are preserved.

Regression tests cover Up, Down, Steps, Migrate and Run failures, a blocked source reader, and early stop. The root package and local file source are checked with the race detector. LinkForge separately checks its PostgreSQL driver, cancellation and resource cleanup with a real database. Other upstream database/source combinations are not independently certified by this fork.

The patch is intentionally limited to resource ownership; it adds no migration features or application-specific behavior. When upstream contains an equivalent fix, LinkForge can remove the replacement after verifying the regressions.
