# Agent Instructions: Bridge Verification Workflow

You are expected to use the Bridge CLI to maintain verification-aware alignment between code, specs, tests, and evidence.

## Core Command
Always use `bb bridge` instead of `clojure -M:bridge` for faster startup.

## Prerequisites
- `bb` (babashka) must be installed.

## Verification Workflow
When the user asks to "verify this change", "check obligations", "run bridge", "analyze change", or after significant code changes and always before committing code, follow these phases:

### Phase 1: Analyze Change
Identify what changed and what obligations arise.
```bash
bb bridge next
```

### Phase 2: Run evidences

List and run evidence.
```bash
bb bridge list-evidence
bb bridge run-evidence --id <id>
```

### Phase 3/1: Iterate until converged

Run `bb bridge next` until no remaining open obligations.

## Important Constraints
- **DO NOT modify .bridge files unless instructed.** Bridge is for tracking and analysis, not for code modification.
- **Convergence is key.** If `bb bridge next` reports `regressed`, you must loop back to Phase 1.
