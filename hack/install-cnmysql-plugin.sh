#!/bin/sh
set -e

usage() {
  cat <<EOF
Usage: $0 [-b bindir] [-d] [tag]

  -b  installation directory (default: ~/.local/bin)
  -d  enable debug output
  tag the release tag to install (default: latest)

Example:
  curl -sSfL https://github.com/CloudNative-MySQL/cloudnative-mysql/raw/main/hack/install-cnmysql-plugin.sh | sh -s -- -b /usr/local/bin

EOF
  exit 2
}

log_info()  { echo "$0: $*" 1>&2; }
log_debug() { [ -n "$DEBUG" ] && echo "$0: $*" 1>&2; }
log_err()   { echo "$0: error: $*" 1>&2; }

parse_args() {
  BINDIR="${BINDIR:-$HOME/.local/bin}"
  while getopts "b:dh" arg; do
    case "$arg" in
      b) BINDIR="$OPTARG" ;;
      d) DEBUG=1 ;;
      h) usage ;;
      *) usage ;;
    esac
  done
  shift $((OPTIND - 1))
  TAG="$1"
}

detect_os() {
  os=$(uname -s | tr '[:upper:]' '[:lower:]')
  case "$os" in
    linux)  echo "linux" ;;
    darwin) echo "darwin" ;;
    *)
      # Windows is supported for downloading but install
      # is untested outside WSL/MSYS2.
      case "$os" in
        cygwin_nt*|mingw*|msys_nt*) echo "windows" ;;
        *) log_err "unsupported OS: $os"; exit 1 ;;
      esac
      ;;
  esac
}

detect_arch() {
  arch=$(uname -m)
  case "$arch" in
    x86_64|amd64) echo "amd64" ;;
    aarch64|arm64) echo "arm64" ;;
    *) log_err "unsupported architecture: $arch"; exit 1 ;;
  esac
}

get_latest_tag() {
  log_info "checking GitHub for latest release"
  tag=$(curl -sSfL "https://api.github.com/repos/CloudNative-MySQL/cloudnative-mysql/releases/latest" \
    | grep '"tag_name":' | sed -E 's/.*"tag_name": *"([^"]+)".*/\1/')
  if [ -z "$tag" ]; then
    log_err "could not determine latest release tag"
    exit 1
  fi
  echo "$tag"
}

install_binary() {
  OS=$(detect_os)
  ARCH=$(detect_arch)

  if [ -z "$TAG" ]; then
    TAG=$(get_latest_tag)
  fi
  VERSION="${TAG#v}"

  if [ "$OS" = "windows" ] && [ "$ARCH" = "arm64" ]; then
    log_err "windows/arm64 is not supported"
    exit 1
  fi

  log_info "installing kubectl-cnmysql ${TAG} for ${OS}/${ARCH}"

  NAME="kubectl-cnmysql_${VERSION}_${OS}_${ARCH}"
  if [ "$OS" = "windows" ]; then
    ARCHIVE="${NAME}.zip"
  else
    ARCHIVE="${NAME}.tar.gz"
  fi

  BASE_URL="https://github.com/CloudNative-MySQL/cloudnative-mysql/releases/download/${TAG}"
  CHECKSUMS_URL="${BASE_URL}/checksums.txt"
  ARCHIVE_URL="${BASE_URL}/${ARCHIVE}"

  TMPDIR=$(mktemp -d)
  trap 'rm -rf "$TMPDIR"' EXIT

  log_debug "downloading ${ARCHIVE_URL}"
  curl -fsSL -o "${TMPDIR}/${ARCHIVE}" "${ARCHIVE_URL}"

  log_debug "downloading ${CHECKSUMS_URL}"
  curl -fsSL -o "${TMPDIR}/checksums.txt" "${CHECKSUMS_URL}"

  log_debug "verifying checksum"
  EXPECTED=$(grep "  ${ARCHIVE}$" "${TMPDIR}/checksums.txt" 2>/dev/null | tr '\t' ' ' | cut -d ' ' -f 1)
  if [ -z "$EXPECTED" ]; then
    log_err "could not find checksum for ${ARCHIVE}"
    exit 1
  fi
  if command -v sha256sum >/dev/null 2>&1; then
    ACTUAL=$(sha256sum "${TMPDIR}/${ARCHIVE}" | cut -d ' ' -f 1)
  elif command -v shasum >/dev/null 2>&1; then
    ACTUAL=$(shasum -a 256 "${TMPDIR}/${ARCHIVE}" | cut -d ' ' -f 1)
  else
    log_err "no sha256 tool found (sha256sum or shasum)"
    exit 1
  fi
  if [ "$EXPECTED" != "$ACTUAL" ]; then
    log_err "checksum mismatch: expected $EXPECTED, got $ACTUAL"
    exit 1
  fi
  log_debug "checksum verified"

  log_debug "extracting ${ARCHIVE}"
  if [ "$OS" = "windows" ]; then
    unzip -qo "${TMPDIR}/${ARCHIVE}" -d "${TMPDIR}"
  else
    tar xzf "${TMPDIR}/${ARCHIVE}" -C "${TMPDIR}"
  fi

  mkdir -p "${BINDIR}"
  install -m 0755 "${TMPDIR}/kubectl-cnmysql" "${BINDIR}/kubectl-cnmysql"
  log_info "installed ${BINDIR}/kubectl-cnmysql"

  cat >"${BINDIR}/kubectl_complete-cnmysql" <<'COMPLETION'
#!/usr/bin/env sh
exec kubectl-cnmysql __complete "$@"
COMPLETION
  chmod +x "${BINDIR}/kubectl_complete-cnmysql"
  log_info "installed ${BINDIR}/kubectl_complete-cnmysql (tab completion)"
}

parse_args "$@"
install_binary
