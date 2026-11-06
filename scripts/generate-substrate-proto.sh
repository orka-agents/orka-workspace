#!/usr/bin/env bash
# Regenerates or verifies the vendored Agent Substrate control protocol and Go
# bindings in providers/substrate/nativepb from the commit pinned in
# providers/substrate/nativepb/upstream.env. The proto source is copied
# unmodified; only the Go import path is remapped on the protoc command line.
set -Eeuo pipefail

root_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
pin_file="${root_dir}/providers/substrate/nativepb/upstream.env"
# shellcheck source=providers/substrate/nativepb/upstream.env
source "${pin_file}"
mode="${1:-generate}"
case "${mode}" in generate|verify) ;; *) printf 'usage: %s [generate|verify]\n' "$0" >&2; exit 2 ;; esac

work_dir="$(mktemp -d "${TMPDIR:-/tmp}/orka-workspace-substrate-proto.XXXXXX")"
trap 'rm -rf "${work_dir}"' EXIT
if [[ -n "${SUBSTRATE_SOURCE_DIR:-}" ]]; then
  source_commit="$(git -C "${SUBSTRATE_SOURCE_DIR}" rev-parse HEAD)"
  [[ "${source_commit}" == "${SUBSTRATE_UPSTREAM_COMMIT}" ]] || { printf 'Substrate source commit does not match the pinned upstream commit\n' >&2; exit 1; }
  cp "${SUBSTRATE_SOURCE_DIR}/${SUBSTRATE_UPSTREAM_PROTO_PATH}" "${work_dir}/ateapi.proto"
else
  curl --fail --show-error --silent --location \
    "https://raw.githubusercontent.com/agent-substrate/substrate/${SUBSTRATE_UPSTREAM_COMMIT}/${SUBSTRATE_UPSTREAM_PROTO_PATH}" \
    --output "${work_dir}/ateapi.proto"
fi
if command -v sha256sum >/dev/null; then
  source_digest="$(sha256sum "${work_dir}/ateapi.proto" | awk '{print $1}')"
else
  source_digest="$(shasum -a 256 "${work_dir}/ateapi.proto" | awk '{print $1}')"
fi
[[ "${source_digest}" == "${SUBSTRATE_UPSTREAM_PROTO_SHA256}" ]] || { printf 'Substrate protocol source digest mismatch\n' >&2; exit 1; }

protoc_bin="${PROTOC:-protoc}"
command -v "${protoc_bin}" >/dev/null || { printf 'protoc is required (set PROTOC to override)\n' >&2; exit 1; }
[[ "$("${protoc_bin}" --version)" == "libprotoc ${SUBSTRATE_PROTOC_VERSION}" ]] || {
  printf 'Protocol generation requires protoc %s\n' "${SUBSTRATE_PROTOC_VERSION}" >&2
  exit 1
}
mkdir -p "${root_dir}/bin"
GOBIN="${root_dir}/bin" go install "google.golang.org/protobuf/cmd/protoc-gen-go@${SUBSTRATE_PROTOC_GEN_GO_VERSION}"
GOBIN="${root_dir}/bin" go install "google.golang.org/grpc/cmd/protoc-gen-go-grpc@${SUBSTRATE_PROTOC_GEN_GO_GRPC_VERSION}"
go_import='Mateapi.proto=github.com/orka-agents/orka-workspace/providers/substrate/nativepb;ateapipb'
PATH="${root_dir}/bin:${PATH}" "${protoc_bin}" --proto_path="${work_dir}" \
  --go_out="${work_dir}" --go_opt=paths=source_relative "--go_opt=${go_import}" \
  --go-grpc_out="${work_dir}" --go-grpc_opt=paths=source_relative "--go-grpc_opt=${go_import}" \
  "${work_dir}/ateapi.proto"

for filename in ateapi.proto ateapi.pb.go ateapi_grpc.pb.go; do
  if [[ "${mode}" == verify ]]; then
    cmp "${work_dir}/${filename}" "${root_dir}/providers/substrate/nativepb/${filename}"
  else
    cp "${work_dir}/${filename}" "${root_dir}/providers/substrate/nativepb/${filename}"
  fi
done
