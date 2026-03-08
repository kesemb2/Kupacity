# CLAUDE.md — AI Assistant Guide for `cinema`

This file provides context, conventions, and workflows for AI assistants (e.g., Claude Code) working in this repository.

---

## Project Overview

**Name:** cinema
**Status:** Early initialization — no source code yet.
**Purpose:** (To be defined as the project evolves.)

The repository currently contains only a placeholder `README.md`. This document should be updated as the project grows.

---

## Repository Structure

```
cinema/
├── CLAUDE.md         # This file
└── README.md         # Project overview (placeholder)
```

As new directories and files are added, update the tree above to reflect the actual structure.

---

## Development Workflows

### Starting Work

1. Always confirm which branch to develop on before making changes.
2. Pull the latest changes before starting: `git pull origin <branch>`
3. Never push directly to `main` or `master` without explicit instruction.

### Committing

- Write clear, descriptive commit messages in the imperative mood (e.g., `Add user authentication`, `Fix movie search pagination`).
- Keep commits focused — one logical change per commit.
- Do not commit generated files, secrets, or environment-specific configuration.

### Pushing

- Push to the feature branch you are working on: `git push -u origin <branch>`
- Do not force-push unless explicitly requested by the user.

---

## Code Conventions

Since no code exists yet, these are the default conventions to follow when adding code:

### General

- Prefer simplicity over cleverness. Write code that is easy to read and reason about.
- Avoid over-engineering. Only add abstractions when they are clearly needed.
- Do not add unused code, dead imports, or placeholder comments.

### File Organization

- Group related code in clearly named directories (e.g., `src/`, `lib/`, `tests/`).
- Keep files small and focused on a single responsibility.

### Naming

- Use descriptive names for variables, functions, files, and directories.
- Follow the naming conventions of the language/framework chosen for the project.

### Testing

- Write tests for new functionality.
- Place tests in a `tests/` (or `__tests__/`, `spec/`) directory mirroring the source structure.
- Ensure all tests pass before pushing.

### Documentation

- Update `README.md` and `CLAUDE.md` as the project evolves.
- Add inline comments only where the logic is non-obvious.

---

## Environment Setup

No environment setup is required yet. When dependencies and configuration are added, document the setup steps here:

```bash
# Example — update this when the stack is chosen
# npm install
# cp .env.example .env
# npm run dev
```

---

## Key Commands

Update this section as scripts are added to the project:

| Command | Description |
|---------|-------------|
| _(none yet)_ | _(no scripts defined)_ |

---

## AI Assistant Instructions

- **Read before editing.** Always read a file before modifying it.
- **Stay in scope.** Only make changes directly relevant to the task.
- **No speculative features.** Do not add functionality that was not requested.
- **Security first.** Never introduce secrets, hardcoded credentials, or known vulnerabilities.
- **Update this file.** When the project structure, stack, or workflows change significantly, update `CLAUDE.md` to reflect the new state.
- **Ask when unclear.** If a requirement is ambiguous, ask the user rather than guessing.

---

## Git Remote

- **Remote:** `origin`
- **Host:** `http://local_proxy@127.0.0.1:40045/git/kesemb2/cinema`
- Feature branches follow the pattern: `claude/<description>-<session-id>`

---

*Last updated: 2026-03-08. Update this file whenever the project state changes significantly.*
