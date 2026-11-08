#!/bin/bash
# Home build: runs as root inside a dedicated Debian trixie WSL2 distro ("moltis-build") on the desktop.
# Output tarball is COPY-ed by the local_moltis HA add-on Dockerfile (sha256-pinned).
# Runs as root inside the dedicated moltis-build WSL distro (Debian trixie).
# Mirrors the builder stage of the upstream Dockerfile, with a CPU-portable feature set:
# the HA host (Celeron N3450, no AVX) cannot run zvec (prebuilt AVX lib) or llama.cpp (GGML_NATIVE).
exec >/mnt/c/wsl/moltis-build/build.log 2>&1
set -euxo pipefail
export HOME=/root DEBIAN_FRONTEND=noninteractive
out=/mnt/c/wsl/moltis-build/out
branch=home/20261023.01
expected_commit=46361fb
export MOLTIS_VERSION=20261023.01
# "portable" minus local-llm*, local-embeddings; "full" would also add zvec
export MOLTIS_BUILD_FEATURES="acp,agent,bundled-skills,caldav,claude-import,cloudflare-tunnel,code-index-builtin,code-splitter,codex-import,connectors,discord,file-watcher,firecrawl,fs-tools,graphql,hermes-import,home-assistant,jemalloc,lang-bash,lang-c,lang-cpp,lang-css,lang-go,lang-html,lang-java,lang-javascript,lang-json,lang-markdown,lang-python,lang-ruby,lang-rust,lang-toml,lang-typescript,langfuse,llm-compaction,matrix,mdns,metrics,msteams,netbird,ngrok,nostr,openclaw-import,otlp,prometheus,provider-github-copilot,provider-kimi-code,provider-openai-codex,push-notifications,qmd,signal,slack,tailscale,telegram,telephony,tls,trusted-network,vault,vercel-sandbox,voice,wasm,web-ui,whatsapp"
echo "BUILD START $(date -Is)"

if ! command -v cmake >/dev/null || ! command -v node >/dev/null; then
  apt-get update -qq
  apt-get install -yqq --no-install-recommends ca-certificates curl wget gnupg git cmake build-essential \
    libclang-dev pkg-config xz-utils unzip file procps libssl-dev zlib1g-dev
  curl -fsSL https://deb.nodesource.com/setup_22.x | bash -
  apt-get install -yqq --no-install-recommends nodejs
fi
command -v qemu-x86_64 >/dev/null || { apt-get update -qq; apt-get install -yqq --no-install-recommends qemu-user; }
[ -x /root/.cargo/bin/rustup ] || curl -fsSL https://sh.rustup.rs | sh -s -- -y --profile minimal --default-toolchain none
. /root/.cargo/env

if [ -d /build/.git ]; then
  if ! git -C /build merge-base --is-ancestor "$expected_commit" HEAD; then
    git -C /build fetch --depth 1 origin "$branch" && git -C /build reset --hard FETCH_HEAD
  fi
else
  git clone --depth 1 -b "$branch" https://github.com/zilewang7/moltis /build
fi
cd /build
# Local home patches not yet pushed to the fork (applied once, idempotent)
for patch in /mnt/c/wsl/moltis-build/patches/*.patch; do
  [ -e "$patch" ] || continue
  subject=$(sed -n 's/^Subject: \[PATCH\] //p' "$patch" | head -1)
  git log --format=%s | grep -qF "$subject" || git -c user.name=home -c user.email=home@local am "$patch"
done
git log -1 --format='commit %H %s'
nightly="$(sed -nE 's/^channel[[:space:]]*=[[:space:]]*"([^"]+)"/\1/p' rust-toolchain.toml)"
rustup toolchain list | grep -q "^${nightly}" || rustup toolchain install "$nightly" --profile minimal --component clippy,rustfmt --target wasm32-wasip2

tw=tailwindcss-linux-x64
[ -x "./$tw" ] || { curl -fsSLO "https://github.com/tailwindlabs/tailwindcss/releases/latest/download/$tw"; chmod +x "$tw"; }
TAILWINDCSS="./$tw" ./scripts/build-web-assets.sh

cargo build --target wasm32-wasip2 -p moltis-wasm-calc -p moltis-wasm-web-fetch -p moltis-wasm-web-search --release
./scripts/cargo-build-moltis.sh --release

# Gate: must start on a Goldmont-class CPU (same family as the N3450, no AVX)
if ldd target/release/moltis | grep -q zvec; then echo "binary still links zvec"; exit 1; fi
qemu-x86_64 -cpu Denverton target/release/moltis --version

# Same artifact set the upstream runtime stage copies (binary, web assets, wasm); no zvec
rm -rf "$out" && mkdir -p "$out/stage/web" "$out/stage/wasm"
cp target/release/moltis "$out/stage/moltis"
cp -r crates/web/src/assets/. "$out/stage/web/"
cp target/wasm32-wasip2/release/moltis_wasm_calc.wasm target/wasm32-wasip2/release/moltis_wasm_web_fetch.wasm \
   target/wasm32-wasip2/release/moltis_wasm_web_search.wasm "$out/stage/wasm/"
name="moltis-${MOLTIS_VERSION}-home-noavx-linux-amd64.tar.gz"
tar -C "$out/stage" -czf "$out/$name" moltis web wasm
(cd "$out" && sha256sum "$name" > SHA256SUMS && cat SHA256SUMS)
echo "BUILD DONE $(date -Is)"
