# Fix Legacy Tests for Cosmos Integration

## Why

After migrating from custom task builders to Astronomer Cosmos, 14 tests fail because Cosmos validates dbt project files exist and some tests use deprecated mock assertion syntax.

## What Changes

- Create dbt project fixtures in `tests/fixtures/dbt_project/` with valid `dbt_project.yml` and `manifest.json`
- Update test config files to point to fixture directory instead of `/dbt`
- Fix mock assertion typos in `test_notifications.py`
- Remove Python 3.8 and 3.12 from test matrix (support only 3.9-3.11)
- Update `tox.ini` envlist to `py39,py310,py311`
- Update `setup.py` classifiers to remove unsupported Python versions

## Impact

- Affected specs: `test-suite` capability
- Affected code:
  - `tests/test_dag_factory.py` (11 tests)
  - `tests/test_notifications.py` (3 tests)
  - `tests/config/base/dbt.yml`
  - `tox.ini`
  - `setup.py`

## Context

**Current State:**
- 35/49 tests passing (all Cosmos unit tests + 1 notification test)
- 14/49 tests failing:
  - 11 `test_dag_factory.py` tests - fail because Cosmos validates dbt project existence
  - 3 `test_notifications.py` tests - have assertion typos

**Completed:**
- ✅ Removed 9 obsolete test files testing deleted code
- ✅ Fixed `ms_teams_webhook_operator.py` import
- ✅ Added `--prefer-binary` to tox.ini to solve google-re2 compilation

## Problem Details

1. **`test_dag_factory.py` (11 tests)**: Tests validate that `AirflowDagFactory` properly reads YAML config and creates DAGs. These tests were written for the old implementation which didn't validate dbt project files. Cosmos now validates that `dbt_project.yml` and `manifest.json` exist, causing tests to fail with `CosmosValueError: Could not find dbt_project.yml at /dbt/dbt_project.yml`.

2. **`test_notifications.py` (3 tests)**: Tests have assertion typo (`mock.called_once` → should be `mock.assert_called_once`) which became an error in newer Python versions.

## Solution Options

### Option A: Update Tests with Real dbt Fixtures (Recommended)
Create minimal dbt project fixtures in `tests/fixtures/` that Cosmos can validate:
- Add `dbt_project.yml` with minimal configuration
- Add `manifest.json` with test models
- Update test configs to point to fixture directory
- Tests validate real end-to-end DAG creation with Cosmos

**Pros:**
- Tests real Cosmos integration behavior
- More confidence in actual functionality
- Future-proof as Cosmos evolves

**Cons:**
- More test maintenance (fixtures to maintain)
- Slightly slower tests (Cosmos validation)

### Option B: Mock Cosmos Validation
Mock out Cosmos's `validate_project()` method in tests to skip file validation.

**Pros:**
- Minimal changes to existing tests
- Fast test execution

**Cons:**
- Not testing real Cosmos behavior
- May miss integration issues
- Fragile (breaks if Cosmos internals change)

**Recommendation:** Option A - Create real fixtures for better integration testing.

## Scope

**In Scope:**
- Fix all 14 failing tests to pass
- Create dbt project fixtures for `test_dag_factory.py`
- Fix notification test assertions
- Ensure tox runs successfully across Python 3.9-3.12
- Update test documentation if needed

**Out of Scope:**
- Adding new test coverage (covered in `add-cosmos-integration` change)
- Performance optimization of tests
- Refactoring test structure

## Dependencies

- **Depends on:** `add-cosmos-integration` change (already in progress)
- **Blocks:** Nothing - this is cleanup work
- **Related:** Phase 5 (Testing) of `add-cosmos-integration`

## Success Criteria

- [ ] All 49 tests pass (35 already passing + 14 fixed)
- [ ] `pytest tests/` exits with code 0
- [ ] `tox -e py39,py310,py311,py312` completes successfully
- [ ] Test coverage remains >80%
- [ ] No test warnings about deprecated code

## Risks & Mitigations

| Risk | Impact | Likelihood | Mitigation |
|------|--------|------------|------------|
| Fixtures don't match real dbt projects | Medium | Low | Use minimal but valid dbt project structure from dbt docs |
| Tests become slow with Cosmos validation | Low | Medium | Keep fixtures minimal; consider caching if needed |
| Cosmos changes break fixtures | Medium | Low | Pin Cosmos version in tests; update fixtures when upgrading |

## Timeline Estimate

- **Total:** 2-3 hours
- Fixture creation: 1 hour
- Test updates: 1 hour
- Validation & cleanup: 0.5-1 hour
