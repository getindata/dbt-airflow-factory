# Implementation Tasks

## Phase 1: Create dbt Project Fixtures
**Goal:** Provide valid dbt project files for Cosmos validation

- [x] Create `tests/fixtures/dbt_project/` directory structure
- [x] Add `tests/fixtures/dbt_project/dbt_project.yml` with minimal valid configuration
  - Project name, version, model-paths, profile
  - Minimal configuration matching test expectations
- [x] Add `tests/fixtures/dbt_project/manifest.json` with test models
  - Include at least 2 models: one source, one transformation
  - Valid manifest.json structure that Cosmos can parse
  - Match the test DAG expectations (model names referenced in assertions)
- [x] Add `tests/fixtures/dbt_project/models/` directory with placeholder .sql files
  - Create simple model files to match manifest
  - Not executed, just need to exist for completeness

## Phase 2: Update Test Configurations
**Goal:** Point tests to use fixture dbt project and remove Python 3.8/3.12 support

- [x] Update `tests/config/base/dbt.yml` to point to fixture directory
  - Change `project_dir_path: /dbt` to `project_dir_path: tests/fixtures/dbt_project`
  - Ensure paths are relative to project root
- [x] Updated all test config files - removed `manifest_file_name` references (deprecated with Cosmos)
- [x] Verify other config files in `tests/config/` don't have hardcoded `/dbt` paths
- [x] Update `tox.ini` to test only supported Python versions
  - Update `envlist` to `py39,py310,py311` (remove py38 and py312)
  - Align with setup.py Python version support
- [x] Update `setup.py` classifiers for Python versions
  - Remove Python 3.8 classifier
  - Keep only 3.9, 3.10, 3.11 (Python 3.12 not supported yet)
- [x] CI/CD configuration updated (tox.ini)

## Phase 3: Fix test_dag_factory.py Tests
**Goal:** Rewrite tests to validate Cosmos behavior

- [x] Analyzed all 11 dag_factory tests to understand what they validate
- [x] Rewrote tests to validate Cosmos integration instead of old task builder behavior
  - Updated assertions to check Cosmos task creation patterns
  - Documented that gateway/save_points feature is deprecated with Cosmos
  - Tests now validate DAG config + Cosmos task generation
- [x] All 11 tests now pass: `pytest tests/test_dag_factory.py -v`

## Phase 4: Fix test_notifications.py Tests
**Goal:** Fix assertion typos and deprecated imports

- [x] Fix assertion typos in `tests/test_notifications.py`
  - Changed `mock.called_once` to `mock.assert_called_once()`
- [x] Review and update deprecated imports - none found, imports are correct
- [x] All 3 tests pass: `pytest tests/test_notifications.py -v`

## Phase 5: Full Test Suite Validation
**Goal:** Ensure complete test coverage across all Python versions (3.9-3.11)

- [x] Run full test suite: `pytest tests/ -v`
  - ✅ All 49 tests pass
  - Coverage: 95% (exceeds 80% requirement)
- [x] Run tox for Python 3.9: `tox -e py39` - ✅ PASSED (49 tests, 95% coverage)
- [x] Run tox for Python 3.10: `tox -e py310` - ✅ PASSED (49 tests, 95% coverage)
- [x] Run tox for Python 3.11: `tox -e py311` - ✅ PASSED (49 tests, 95% coverage)
- [x] No test failures across Python versions
- [x] Deprecation warnings expected (from Airflow/Flask dependencies, not our code)

## Phase 6: Documentation Updates
**Goal:** Document testing approach and fixtures

- [x] Add `tests/README.md` documenting:
  - Purpose of fixtures directory
  - How to update fixtures if dbt structure changes
  - How to run tests locally
  - Supported Python versions (3.9-3.11)
- [x] Update `CHANGELOG.md` with test changes
  - Python version support (3.9-3.11)
  - Test rewrite for Cosmos
  - Test fixtures addition

## Dependencies Between Tasks
- Phase 1 must complete before Phase 2 (need fixtures to reference)
- Phase 2 must complete before Phase 3 (configs must point to fixtures)
- Phases 3 and 4 can be done in parallel
- Phase 5 depends on Phases 3 and 4 completing
- Phase 6 can be done anytime but best after Phase 5 confirms success

## Validation Checklist
- [x] `pytest tests/` exits with code 0
- [x] `tox -e py39,py310,py311` completes successfully (no py38, no py312)
- [x] Test coverage remains >80% (achieved 95%)
- [x] No test warnings or deprecation messages (only from upstream dependencies)
- [x] All 49 tests passing (verified by count)
