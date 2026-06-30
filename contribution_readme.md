# Contribution 1: "feat: Support nested replication keys"

**Contribution Number:** 1  
**Student:** Daria Hrabar  
**Issue:** https://github.com/meltano/sdk/issues/1198  
**Status:** Phase IV In Progress

---

## Why I Chose This Issue

This issue entails implementing support for nested replication keys to track incremental data updates. By enabling the SDK to parse deep JSON paths directly, this update optimizes data pipelines and removes the need for manual data restructuring.

As a developing software engineer, I chose this issue to learn best practices for contributing to open-source projects in a welcoming environment. I am also excited to expand my knowledge of Python, JSON, API, and AI. Lastly, I hope to make a positive impact on the GitHub community by offering a solution that can be integrated into the main codebase.

---

## Understanding the Issue

### Problem Description

The Meltano Singer SDK only supports flat replication keys — field names that sit at the top level of a record, like `"updated_at"`. When a developer tries to use a dotted path like `"attributes.updated"` to access a timestamp nested within a sub-object, the SDK cannot find it. This means taps whose APIs return timestamps inside nested objects cannot use incremental replication without workarounds.

### Expected Behavior

When a developer sets `replication_key = "attributes.updated"`, the SDK should interpret the dot as a path separator and traverse the record to retrieve the value at `record["attributes"]["updated"]`. The retrieved timestamp should then be used to update the stream's bookmark in state, so the next sync only fetches records newer than that value.

### Current Behavior

The SDK performs a flat dictionary lookup `record.get("attributes.updated")`, which returns `None` because no top-level key with that exact name exists. As a result, the state is never updated, and the tap either re-syncs all records on every run or silently fails to track progress.

### Affected Components

The primary file is `singer_sdk/streams/core.py`, specifically the `_increment_stream_state()` method, where the replication key value is extracted from each record, and the `is_timestamp_replication_key` property, where the schema is validated against the key. The state management logic in `singer_sdk/streams/_state.py` is also involved, as it receives the key and performs its own record lookup.

---

## Reproduction Process

### Environment Setup

*Step 1:* Install project prerequisites on your local device. More details can be found on [the Prerequisites page](https://docs.meltano.com/contribute/prerequisites/) within the Meltano Documentation.
  1. [Python 3.10+](https://www.python.org/downloads/)
  2. [uv](https://docs.astral.sh/uv/)
  3. [Node 18+](https://nodejs.org/)
  4. [Yarn](https://yarnpkg.com/)
  5. Although not listed as an official prerequisite, it is also recommended to install [Visual Studio Build Tools for C++](https://visualstudio.microsoft.com/visual-cpp-build-tools/) to avoid the ModuleNotFoundError.

*Step 2:* Complete the setup of your local development environment.
  1. Clone the forked repository to your local device by either:
     - Running `git clone git@github.com:[your/github/file/path].git` and `cd sdk` in your terminal, or
     - Completing the process directly through GitHub Desktop.
  2. Install Nox and pre-commit by running `uv tool install nox` and `uv tool install pre-commit`.
  3. If not already there, navigate to the cloned local repository and install dependencies by running `uv sync`.
  4. Install pre-commit hooks by running `pre-commit install --install-hooks`. 

When working in VS Code, the virtual environment should become activated automatically. If not, run the `.venv\Scripts\Activate.ps1` command in your terminal. Your .venv is activated if you can see (singer-sdk) displayed at the beginning of your file path in the terminal window.

### Steps to Reproduce

*Setting up the issue reproduction:*

1. In the root of your local sdk folder, create a new Python file called `reproducing_issue_1198.py`.
2. In the file, define a stream with a nested schema, meaning one field `attributes` contains sub-fields `created` and `updated` inside it, rather than all fields sitting at the top level.
3. Set the stream's `replication_key` to `"attributes.updated"`—a dotted path pointing to the nested timestamp field.
4. Add at least two fake records where the updated timestamp is stored inside attributes, not at the top level of the record.
5. Add a print statement that shows what value the SDK resolves when it looks up the replication key against each record.

*Running the reproduction:*

1. Confirm you are inside the sdk folder before running terminal commands.
2. Confirm the virtual environment is active — your prompt should start with (singer-sdk).
3. Run the file with `python reproducing_issue_1198.py`.
4. Observe the output for both records in the terminal.

*Confirming the bug:*

1. Check that the printed value for `replication_key` shows `"attributes.updated"`—the dotted path you set.
2. Check that the printed SDK resolved value shows `None` for both records—this is the bug. The SDK cannot find the nested field and returns nothing instead of the expected timestamp.

*Verifying the issue is consistent:*

1. Run `python reproducing_issue_1198.py` a second time without changing anything.
2. Confirm the output is identical — both records return `None` again, proving the failure is consistent and not random.

### Reproduction Evidence

- *Issue Reproduction File Name:* `reproducing_issue_1198.py` (demonstrates the original bug using mock nested records)
- *Issue Reproduction Commit:* [feat(streams): add reproduction script for nested replication keys meltano#1198](https://github.com/daria-hrabar/sdk/commit/db4519ce800bf2ac2305216f3a560f259c22933b)

*Sample VS Code Terminal Output After Running `reproducing_issue_1198.py`:*


ISSUE #1198 — Nested Replication Key Reproduction


Record: {'id': 1, 'attributes': {'created': '2024-01-01T00:00:00Z', 'updated': '2024-01-10T00:00:00Z'}}

replication_key = 'attributes.updated'

SDK resolved value = None

✗ BUG CONFIRMED: SDK cannot resolve nested key.

Expected: '2024-01-10T00:00:00Z' (or similar)

Got: None — state tracking will silently fail.
  

Record: {'id': 2, 'attributes': {'created': '2024-02-01T00:00:00Z', 'updated': '2024-02-15T00:00:00Z'}}

replication_key = 'attributes.updated'

SDK resolved value = None

✗ BUG CONFIRMED: SDK cannot resolve nested key.

Expected: '2024-01-10T00:00:00Z' (or similar)

Got: None — state tracking will silently fail.

---

## Solution Approach

### Analysis

The root cause is a flat dictionary lookup in `singer_sdk/streams/core.py`. When the SDK needs to extract a replication key value from a record, it calls `record.get(replication_key)`, treating the key as a literal string. For a dotted key like `"attributes.updated"`, this looks for a top-level field named exactly `"attributes.updated"` — which does not exist — and returns `None`. The same flat lookup issue exists in `is_timestamp_replication_key`, where `schema.get("properties", {}).get(replication_key)` fails to find nested fields in the schema, incorrectly raising `InvalidReplicationKeyException` even when the field exists at a deeper level.

### Proposed Solution

Add a `get_nested_value()` helper function to `singer_sdk/helpers/_util.py` that splits a dotted key on `"."` and traverses a dictionary level by level, returning `None` safely if any level is missing or not a dictionary. Update `_increment_stream_state()` in `core.py` to use this helper to extract the nested value before passing it to the state manager as a flat record it can look up correctly. Update `is_timestamp_replication_key` in the same file to walk the schema's nested `properties` blocks using the same dotted path logic, so nested fields are correctly identified as datetime types rather than raising an exception.

### Using UMPIRE framework (adapted):

**Understand:**
  - Currently, the SDK only supports flat replication keys—single field names like `"updated_at"` that sit at the top level of a record.
  - When a developer sets `replication_key = "attributes.updated"` to point to a nested field, the SDK treats it as a literal dictionary key lookup `record.get("attributes.updated")`, which returns `None` because no top-level key named `"attributes.updated"` exists.
  - As a result, state tracking silently fails—the SDK never bookmarks where it left off, causing either full re-syncs or missed records on every run.
  - What should happen instead: `"attributes.updated"` should be interpreted as a dotted path, traversing `record["attributes"]["updated"]` to retrieve the actual timestamp value.

**Match:**
  - The root lookup happens in `singer_sdk/streams/core.py` inside `_increment_stream_state()`, which calls `self.state_manager.increment_state(latest_record, replication_key=self.replication_key)`, passing the key as a flat string.
  - The schema validation for the replication key is in the `is_timestamp_replication_key property` in the same file—it does `schema.get("properties", {}).get(self.replication_key)`, which also only works for flat keys.

**Plan:**
  1. Add a helper function (e.g., `get_nested_value(record, dotted_key))` that splits the key on "." and traverses the record dict level by level, returning `None` if any level is missing.
  2.  Modify `_increment_stream_state()` in `singer_sdk/streams/core.py` to use the helper when extracting the replication key value from each record, instead of a flat lookup.
  3.  Modify the `is_timestamp_replication_key` property in the same file to traverse the schema using the dotted path (e.g., `walking properties` → `attributes` → `properties` → `updated`) instead of a single-level `schema["properties"].get(key)`.
  4.  Update `singer_sdk/streams/_state.py`, where `increment_state` does its own record lookup, to also use the nested helper.

**Implement:**
  - *Working Branch Link:* [daria-hrabar/sdk at fix-issue-1198](https://github.com/daria-hrabar/sdk/tree/fix-issue-1198)

**Review:**
  - Run unit tests with `nox -r` and pre-commit hooks with `pre-commit run --all` before submitting.
  - PR title must follow conventional commit format: `feat(streams): support nested replication keys via dotted path (#1198)`.

**Evaluate:**
  - Update `reproducing_issue_1198.py` to assert the resolved value equals the expected timestamp. If the fix works, the output changes from `None` to a datetime stamp.
  - Write a formal test in `tests/` that creates a stream with a nested replication key and asserts the state is correctly incremented after processing records.
  - Run `nox -rs` tests to execute the core test suite and confirm nothing else is broken.
  - Manually run the reproduction script twice and confirm the bug no longer exists.

---

## Testing Strategy

### Unit Tests

- *Test case 1:* Flat replication key — no regression. Verifies that existing flat keys like `updated_at` still resolve correctly after the fix, confirming no existing behavior was broken.
- *Test case 2:* Two-level nested key traversal. Verifies that a dotted path like `properties.hs_lastmodifieddate` correctly traverses two dictionary levels and returns the timestamp value.
- *Test case 3:* Three-level nested key traversal. Verifies that a path like `meta.audit.last_modified` traverses three levels correctly, confirming the fix is not limited to two levels.
- *Test case 4:* Missing final key returns `None` safely. Verifies that when the last key in the path doesn't exist, the function returns `None` without crashing — e.g., `properties.nonexistent_field`.
- *Test case 5:* Missing intermediate key returns `None` safely. Verifies that when an intermediate key doesn't exist at all, the function returns `None` — e.g., `organization.updated_at` on a record with no organization field.
- *Test case 6:* Non-dictionary intermediate value returns `None` safely. Verifies that when an intermediate value is a string instead of a dict (e.g., `{"attributes": "corrupted-string"}`), the function returns `None` instead of raising an AttributeError.
- *Test case 7:* Empty record returns `None` safely. Verifies behavior against an empty record `{}`.
- *Test case 8:* `None` intermediate value returns `None` safely. Verifies that `{"properties": None}` does not crash when traversed.
- *Test case 9:* Integer intermediate value returns `None` safely. Verifies that when an intermediate value is an integer rather than a dictionary, the function returns `None` instead of raising an `AttributeError`, guarding against malformed API responses where a numeric value appears where an object is expected.
- *Test case 10:* Invalid nested replication key raises `InvalidReplicationKeyException`. Verifies that when a dotted replication key points to a field that does not exist anywhere in the stream's schema, the `is_timestamp_replication_key` property correctly raises `InvalidReplicationKeyException`, confirming the schema validation error path still works correctly after the fix.

### Integration Tests

- *Integration scenario 1:* The `get_nested_value()` helper correctly resolves nested timestamp values from mock records — confirmed via `verifying_fix_1198.py`. Direct automated verification that the state bookmark advances after processing nested records was not implemented as a standalone unit test, since doing so cleanly requires a stream with a nested schema definition, which falls outside the scope of the existing `SimpleTestStream` fixture. Instead, the bookmark advancement logic is covered indirectly: `get_nested_value()` is proven to return the correct timestamp value, and `_increment_stream_state()` passes that value to `increment_state()` unchanged. End-to-end bookmark advancement would be confirmed during the maintainer review of the PR.
- *Integration scenario 2:* Full test suite run via `nox -s tests` across all supported Python versions (3.10–3.14) confirms no existing stream, state, or replication behavior was broken by the changes to `core.py` and `_util.py`.

### Manual Testing

Created and ran `verifying_fix_1198.py`. Both sample records resolved correctly after the fix was applied.

*Sample VS Code Terminal Output:*


Record: {'id': 1, 'attributes': {'created': '2024-01-01T00:00:00Z', 'updated': '2024-01-10T00:00:00Z'}}

replication_key = 'attributes.updated'

Resolved value  = '2024-01-10T00:00:00Z'

Expected value  = '2024-01-10T00:00:00Z'

✓ PASS: nested key resolved correctly.


Record: {'id': 2, 'attributes': {'created': '2024-02-01T00:00:00Z', 'updated': '2024-02-15T00:00:00Z'}}

replication_key = 'attributes.updated'

Resolved value  = '2024-02-15T00:00:00Z'

Expected value  = '2024-02-15T00:00:00Z'

✓ PASS: nested key resolved correctly.


This confirms that the new `get_nested_value()` helper function traverses the dotted path correctly and returns the actual timestamp instead of `None`.

---

## Implementation Notes

### Week 1 Progress

**What Was Built:**
- Added `get_nested_value()` helper function to `singer_sdk/helpers/_util.py` to traverse nested dictionary paths using a dotted key string.
- Updated `is_timestamp_replication_key` in `singer_sdk/streams/core.py` to traverse nested schema `properties` blocks using a dotted key path, so the property can correctly identify whether a nested field like `"attributes.updated"` is a datetime type instead of incorrectly raising InvalidReplicationKeyException when the field exists but is not at the top level of the schema.
- Updated `_increment_stream_state()` in `singer_sdk/streams/core.py` to extract the nested value before passing it to the state manager, building a flat record the state manager can look up without modification.
- Added formal unit tests to `tests/core/test_streams.py` covering happy path and major edge cases.
- Created `verifying_fix_1198.py` in `tests/core` to confirm the solution is effective using mock nested records.

**Challenges Faced:**
- GitHub Desktop pre-commit hook triggered InvalidManifestError on commit. Resolved by committing directly via Windows PowerShell instead.
- `nox -s tests` failed across all Python versions with `os error 396` hardlink failures caused by the repo living inside a OneDrive-synced folder. Resolved by setting `UV_LINK_MODE=copy` permanently via `[System.Environment]::SetEnvironmentVariable("UV_LINK_MODE", "copy", "User")`, then clearing stale nox environments with `Remove-Item -Recurse -Force .nox\tests-3-10, .nox\tests-3-11, .nox\tests-3-12, .nox\tests-3-13` to force a clean reinstall on the next run.
- Multiple Ruff pre-commit errors required iterative fixes: missing docstrings, missing type annotations, banned print() statements (resolved with `# noqa: T201`), and incorrect typing import conventions (resolved by using `import typing as t` and `t.TYPE_CHECKING` throughout).

**Windows PowerShell Commands Used:**
- `git add <file>` — stages specific files for commit
- `git commit -m "title" -m "description"` — commits with both title and body
- `git push origin <branch-name>` — pushes local commits to the remote fork
- `git pull origin <branch-name> --rebase` — syncs local branch with remote without creating a merge commit
- `nox -s tests` — runs the full test suite across all supported Python versions
- `pre-commit run --all` — runs all code quality checks manually before committing

**VS Code Extension Installed:**

*Ruff extension* — provides real-time linting and auto-fix commands directly in the editor, eliminating most pre-commit failures before committing. Key commands used:
- `>Ruff: Fix All` — auto-fixes all Ruff errors in the current file
- `>Format Document` — applies Black formatting
- `>Organize Imports` — sorts and cleans up import statements

### Week 2 Progress

**What Was Built:**

- Added two new unit tests to `tests/core/test_streams.py` to expand coverage: one verifying that an integer intermediate value returns `None` safely (`test_get_nested_value_integer_intermediate_returns_none`), and one verifying that an invalid nested replication key correctly raises `InvalidReplicationKeyException` (`test_invalid_nested_replication_key_raises`).
- Moved `verifying_fix_1198.py` from `tests/core/` to the repo root alongside `reproducing_issue_1198.py`.
- Re-ran `nox -s tests` after committing new unit tests, confirming 825 collected items and all sessions passing across Python 3.10–3.14 in under 2 minutes — significantly faster than the first full run due to cached nox environments.

**Challenge Faced:**

Determined that `_increment_stream_state()` end-to-end testing requires a stream with a nested schema, which is not available in the existing `SimpleTestStream` fixture — deferred this to maintainer feedback rather than introducing new fixture classes unnecessarily.

**Windows PowerShell Output After The Latest Nox Test Suite Run:**


nox > Session coverage was successful in 3 seconds.

nox > Ran 6 sessions in 2 minutes:

nox > * tests-3.10: success, took 20 seconds

nox > * tests-3.11: success, took 21 seconds

nox > * tests-3.12: success, took 22 seconds

nox > * tests-3.13: success, took 19 seconds

nox > * tests-3.14: success, took 17 seconds

nox > * coverage: success, took 3 seconds

**Test coverage improvements from Week 1 to Week 2:**

- `singer_sdk/streams/core.py` — increased from 89% to 90% as a result of the new `test_invalid_nested_replication_key_raises` test covering additional lines in the `is_timestamp_replication_key` schema traversal logic.
- `singer_sdk/streams/_state.py` — maintained at 100% with no regressions.
- `singer_sdk/helpers/_util.py` — remained at 89%.

### Code Changes

**Files Modified:**
- `singer_sdk/helpers/_util.py` — added `get_nested_value()` helper
- `singer_sdk/streams/core.py` — updated `is_timestamp_replication_key` and `_increment_stream_state()`
- `tests/core/test_streams.py` — added unit tests and Ruff-required type annotation fixes
- `verifying_fix_1198.py` — fix verification script (root of repo)

**Key Commits:**
- [feat(streams): add get_nested_value helper for dotted key traversal](https://github.com/daria-hrabar/sdk/commit/a93dd9a92857aa3004e26dc4f76e9acffec8e580)
- [test(streams): add verification script confirming nested replication key fix](https://github.com/daria-hrabar/sdk/commit/e311a7a59ace0aa339a82b97f6886356db5f114b)
- [test(streams): add unit tests for nested replication key resolution](https://github.com/daria-hrabar/sdk/commit/c600b3e743b6eb095349dc8cb0ed0d86ad8556d1)
- [test(streams): add invalid nested key and integer intermediate edge case tests](https://github.com/meltano/sdk/commit/bde6e791a030d0d357c0077913e2b79952408714)

**Approach Decisions:**
- Placed `get_nested_value()` in `_util.py` rather than inline in `core.py` so both `core.py` and `_state.py` can import it without circular dependencies, and to keep the helper independently testable.
- Chose to flatten the record before passing to `increment_state()` rather than modifying `_state.py` directly, keeping the change smaller and limiting the risk of breaking other state management behavior.
- Used `# noqa: T201` on all `print()` statements in reproduction scripts rather than replacing them with logging, since these are standalone scripts not part of the library itself.
- Inherited `SimpleTestStream` properties from `conftest.py` in new unit tests rather than creating new top-level stream and tap classes, keeping test code consistent with the existing patterns used throughout `test_streams.py` and avoiding unnecessary fixture bloat.
- Returned `None` in edge case unit tests (missing keys, non-dictionary intermediate values, empty records) rather than raising exceptions, because `get_nested_value()` is called once per record during a live sync — raising an exception on every malformed record would crash the entire pipeline, whereas returning `None` allows the state manager to simply skip advancing the bookmark for that record safely.
- Reused cached nox environments on subsequent test runs by not clearing `.nox/` between runs, reducing `nox -s tests` execution time from 8 minutes on the first run to 2 minutes on repeat runs.

---

## Pull Request

**PR Link:** [feat(streams): support nested replication keys via dotted path #3690](https://github.com/meltano/sdk/pull/3690)

**PR Description:**
---

## What does this PR do?

Adds support for nested replication keys in the Singer SDK by introducing a `get_nested_value()` helper in `singer_sdk/helpers/_util.py` that traverses a record dictionary level by level using a dotted key path (e.g., `"attributes.updated"`). The `_increment_stream_state()` method and `is_timestamp_replication_key` property in `singer_sdk/streams/core.py` are updated to use this traversal instead of a flat dictionary lookup, enabling tap developers to point replication keys at timestamp fields nested inside sub-objects rather than only at top-level fields.

## Why was this PR needed?

Many real-world APIs — including HubSpot, Salesforce, and GitHub — return timestamps nested inside sub-objects. Before this fix, setting `replication_key = "attributes.updated"` caused the SDK to perform a flat lookup (`record.get("attributes.updated")`), which returned `None` because no top-level key with that literal name exists. As a result, the state was never updated, and the tap silently re-synced all records on every run.

Investigation traced the root cause to two locations in `singer_sdk/streams/core.py`:

- `_increment_stream_state()` passed the raw record directly to `increment_state()`, which performed a flat key lookup returning `None` for dotted paths
- `is_timestamp_replication_key` called `schema.get("properties", {}).get(replication_key)` which only checked the schema's top level, incorrectly raising `InvalidReplicationKeyException` even when the field existed at a deeper level

The fix keeps `singer_sdk/streams/_state.py` unchanged by flattening the extracted value into a flat record before passing it to the state manager, limiting the scope of the change and reducing regression risk.

## What are the relevant issue numbers?

Closes #1198

## Does this PR meet the acceptance criteria?

- [x] Tests added for new/changed behavior
- [x] All tests passing
- [x] Follows project style guide
- [x] No breaking changes introduced

## Summary by Sourcery

Support incremental replication using dotted-path replication keys that point to nested timestamp fields, updating schema validation, state management, and tests to reliably handle nested structures.

New Features:
- Support nested replication keys for stream replication using dotted key paths.
- Add a get_nested_value helper for resolving values from nested record dictionaries via dotted paths.

Bug Fixes:
- Ensure timestamp replication key validation correctly traverses nested schema properties instead of only top-level fields.
- Fix state increment logic so replication keys referencing nested fields update bookmarks rather than always returning None.

Enhancements:
- Improve type hints and docstrings in stream tests and core stream methods for clarity.

Tests:
- Add unit tests covering nested replication key resolution, edge cases, and invalid schema configurations.
- Introduce reproduction and verification scripts for Issue #1198 to demonstrate the previous bug and the applied fix.

---

**Maintainer Feedback:**
- 6/30/2026:

*Sourcery AI* identified two high-level concerns and left two specific code comments. At the top level, it flagged that `reproducing_issue_1198.py` and `verifying_fix_1198.py` look like ad-hoc debugging helpers sitting at the repo root and suggested moving them into an `examples/` directory or excluding them from the package entirely. It also noted that `is_timestamp_replication_key` assumes every intermediate schema key corresponds to an object with a `properties` block, and that schemas using `additionalProperties` or arrays of objects may not be handled correctly. In the code comments, it suggested broadening the `get_nested_value()` input type from `dict` to `t.Mapping[str, t.Any]` for wider usability across the SDK, and introducing a `missing` sentinel parameter to allow callers to distinguish between a path that doesn't exist and a path whose leaf value is explicitly `None`. It also requested tests covering `_increment_stream_state()` directly — verifying that flat keys pass the original record unchanged and nested keys pass a flattened record to the state manager — suggesting a `_FakeStateManager` stub approach to avoid coupling to the state implementation.

*Codecov* reported that patch coverage is `88%` with 3 lines in the changed code missing coverage, specifically 2 missing lines and 1 partial branch in `singer_sdk/streams/core.py`. Overall project coverage dropped slightly from `94.21%` to `94.19%`.

*CodSpeed* confirmed that merging this PR will not alter performance, with all 14 existing benchmarks untouched.

**Status:** Awaiting additional review / Iterating

---

## Learnings & Reflections

### Technical Skills Gained

[What you learned technically]

### Challenges Overcome

[What was hard and how you solved it]

### What I'd Do Differently Next Time

[Reflection on your process]

---

## Resources Used

- [Link to helpful documentation]
- [Tutorial or Stack Overflow post that helped]
- [GitHub issues or discussions that helped]
