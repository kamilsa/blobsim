# syntax=docker/dockerfile:1
#
# Builds the `blob-sim` binary and lays it onto a Shadow-capable base image so the
# simulation can run under Shadow inside a container. Built and run by
# `shadow-sim.py` (via `uv run shadow-sim.py`). arm64 only — see blobsim.toml
# [docker].shadow_image and the README's "Running under Shadow (Docker)" section.
#
# SHADOW_IMAGE is the base that ships the Shadow simulator (declared before the
# first FROM so it is usable in a FROM; wired to blobsim.toml [docker].shadow_image
# by shadow-sim.py). blob-sim needs a Shadow with the tcp_sendUserData 65535-byte
# send cap removed (patched shadow-arm fork, commit ae04b0890) or its ≥64 KiB EL
# frames deadlock; the :tcpfix Docker Hub tag carries that fix. Beware BuildKit
# resolves tags from Docker Hub over local images — use a pinned tag, not :latest.
ARG SHADOW_IMAGE=kamilsa/shadow-arm:tcpfix

# ── Stage 1: compile blob-sim (release) ──────────────────────────────────────
FROM --platform=linux/arm64 rust:1-bookworm AS build
WORKDIR /src
# The whole repo is the build context (Cargo.toml/Cargo.lock, src/, and the
# vendored patch/quinn-udp override are all required to build).
COPY . .
RUN cargo build --release

# ── Stage 2: blob-sim on top of the Shadow base image ────────────────────────
FROM --platform=linux/arm64 ${SHADOW_IMAGE}
COPY --from=build /src/target/release/blob-sim /opt/blobsim/blob-sim
