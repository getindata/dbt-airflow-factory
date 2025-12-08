<!-- OPENSPEC:START -->
# OpenSpec Instructions

These instructions are for AI assistants working in this project.

Always open `@/openspec/AGENTS.md` when the request:
- Mentions planning or proposals (words like proposal, spec, change, plan)
- Introduces new capabilities, breaking changes, architecture shifts, or big performance/security work
- Sounds ambiguous and you need the authoritative spec before coding

Use `@/openspec/AGENTS.md` to learn:
- How to create and apply change proposals
- Spec format and conventions
- Project structure and guidelines

Keep this managed block so 'openspec update' can refresh the instructions.

<!-- OPENSPEC:END -->

## Code Guidelines

### Configuration Loading

**Always use `read_config()`, never `read_env_config()` in application code.**

`read_config()` merges configurations from both `base/` and environment-specific directories (e.g., `dev/`, `prod/`). This is critical for backward compatibility where common configs (secrets, resources) live in `base/` and environment-specific overrides live in `dev/`/`prod/`.

```python
# ✅ CORRECT - merges base/ + env/
config = read_config(dag_path, env, "k8s.yml")

# ❌ WRONG - only reads env/, ignores base/
config = read_env_config(dag_path, env, "k8s.yml")
```

**How `read_config()` works:**

1. Reads `config/base/{file_name}` → base configuration
2. Reads `config/{env}/{file_name}` → environment-specific overrides
3. Merges them (env values override base values)

**When to use `read_env_config()`:**
Only as an internal helper within `config_utils.py`. Never in application code.

### Code Comments

**Use minimal, essential comments only.**

Add comments for:

- Non-obvious design decisions
- Critical developer-to-developer information
- Why something is done a certain way (not what)

Avoid comments that:

- Restate what the code does
- Explain standard language features
- Document information already in docstrings

```python
# ✅ GOOD - explains WHY
# Use read_config (not read_env_config) to merge base/ + env/ directories
config = read_config(dag_path, env, "k8s.yml")

# ❌ BAD - restates WHAT the code does
# Read the k8s configuration file from the dag path and environment
config = read_config(dag_path, env, "k8s.yml")
```
- NEVER comment too much - avoid excessive comments, avoid commenting what the code does when the same code is right there to see, only add surgical, important, developer to developer code comments, excessive commenting makes code hard to read and shows usage of codeing assistant