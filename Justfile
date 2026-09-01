# Tasks for this published mirror of the Sentinel Elixir Probe SDK (#160, ADR-0032).
#
# Run inside the Devbox shell: `devbox install` once, then `devbox shell`. These
# recipes reuse the canonical gate names of Sentinel's source repository, scoped to
# this one language. The generated proto modules under gen/ are committed here and on
# the elixirc path via mix.exs, so no recipe generates anything; regenerating is
# described in CONTRIBUTING.md. The test seed is pinned: hermetic at
# test-execution time, no wall clock (the same contract as the source repository).

build: _deps
    mix compile

test: _deps
    mix test --seed 0

# `mix format` reads .formatter.exs (no import_deps, no plugins) and never loads
# dependency paths, so the format gates stay free of the Hex fetch — the same
# split the source repository makes.
lint:
    mix format --check-formatted

fmt-check:
    mix format --check-formatted

# Hex bootstrap plus dependency fetch against the committed mix.lock, which
# resolves nothing and pins everything.
[private]
_deps:
    mix local.hex --force
    mix deps.get
