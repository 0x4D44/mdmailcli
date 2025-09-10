# Repository Guidelines

## Project Structure & Module Organization
- Root: `Cargo.toml` (crate metadata) and `src/` (source).
- Entry point: `src/main.rs` (CLI binary). Add modules as `src/<module>.rs` or folders with `mod.rs`.
- Integration tests (if added): `tests/` with `*_test.rs` files. Artifacts build to `target/`.

## Build, Test, and Development Commands
- Build: `cargo build` (debug) or `cargo build --release` (optimized).
- Run: `cargo run -- <args>` (passes args to the CLI).
- Test: `cargo test` (unit/integration tests).
- Lint: `cargo clippy --all-targets --all-features -D warnings`.
- Format: `cargo fmt --all`.

## Coding Style & Naming Conventions
- Use `rustfmt` defaults (4‑space indent, standard import/order). Run formatting before commits.
- Naming: `snake_case` for functions/modules, `CamelCase` for types/traits, `SCREAMING_SNAKE_CASE` for constants.
- Files: single binary in `src/main.rs`. Additional binaries go in `src/bin/<name>.rs`.
- Keep functions small, return `Result<_, anyhow::Error>` (or crate‑specific error) for fallible paths.

## Testing Guidelines
- Prefer unit tests co‑located with code via `mod tests { ... }` and `#[cfg(test)]`.
- Integration tests live in `tests/` and use public APIs of the binary/library.
- Name tests descriptively (e.g., `parses_minimal_input`, `handles_invalid_flag`).
- Aim for meaningful coverage of CLI flags, error paths, and I/O boundaries.

## Commit & Pull Request Guidelines
- Use Conventional Commits (e.g., `feat: add send subcommand`, `fix: handle empty input`).
- One focused change per PR; include a brief description, linked issue, and usage notes/examples.
- Pre-submit checklist: `cargo fmt`, `cargo clippy -D warnings`, `cargo test`, and update docs if behavior changes.

## Security & Configuration Tips
- Do not commit secrets or tokens. Use environment variables for local testing.
- Validate and sanitize all external input (args, files, env).

## Agent-Specific Instructions
- Make minimal, targeted changes; do not refactor unrelated areas.
- Follow the above commands and style. If adding features, include tests and update this guide as needed.

## Quality Gates
- All code must compile cleanly without warnings (`cargo build`).
- Formatting must be clean (`cargo fmt --all` produces no diffs).
- Clippy must be clean (`cargo clippy --all-targets --all-features -D warnings`).
