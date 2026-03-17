#!/usr/bin/env bash
# PunkGo installer — downloads punkgo-jack + punkgo-kerneld pre-built binaries.
# Usage: curl -fsSL https://raw.githubusercontent.com/PunkGo/punkgo-jack/main/install.sh | bash
set -euo pipefail

JACK_REPO="PunkGo/punkgo-jack"
KERNEL_REPO="PunkGo/punkgo-kernel"

# Default install directory: platform-aware.
if [ -z "${PUNKGO_INSTALL_DIR:-}" ]; then
  case "$(uname -s)" in
    MINGW*|MSYS*|CYGWIN*)
      # Windows: install to ~/.punkgo/bin/ (always writable, no sudo).
      # Use $HOME (POSIX path, consistent with Git Bash $PATH format).
      INSTALL_DIR="$HOME/.punkgo/bin" ;;
    *)
      INSTALL_DIR="/usr/local/bin" ;;
  esac
else
  INSTALL_DIR="$PUNKGO_INSTALL_DIR"
fi

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
DIM='\033[2m'
RESET='\033[0m'

info()  { printf "${GREEN}▸${RESET} %s\n" "$*"; }
warn()  { printf "${RED}▸${RESET} %s\n" "$*"; }
dim()   { printf "${DIM}  %s${RESET}\n" "$*"; }

# Detect OS and architecture
detect_platform() {
  local os arch

  case "$(uname -s)" in
    Linux*)  os="linux" ;;
    Darwin*) os="macos" ;;
    MINGW*|MSYS*|CYGWIN*) os="windows" ;;
    *) warn "Unsupported OS: $(uname -s)"; exit 1 ;;
  esac

  case "$(uname -m)" in
    x86_64|amd64)  arch="x86_64" ;;
    aarch64|arm64) arch="aarch64" ;;
    *) warn "Unsupported architecture: $(uname -m)"; exit 1 ;;
  esac

  # Map to Rust target triple
  case "${os}-${arch}" in
    linux-x86_64)   TARGET="x86_64-unknown-linux-gnu" ;;
    linux-aarch64)  TARGET="aarch64-unknown-linux-gnu" ;;
    macos-x86_64)   TARGET="x86_64-apple-darwin" ;;
    macos-aarch64)  TARGET="aarch64-apple-darwin" ;;
    windows-x86_64) TARGET="x86_64-pc-windows-msvc" ;;
    *) warn "Unsupported platform: ${os}-${arch}"; exit 1 ;;
  esac

  OS="$os"
  ARCH="$arch"
}

# Get latest release tag from GitHub API
latest_tag() {
  local repo="$1"
  curl -fsSL "https://api.github.com/repos/${repo}/releases/latest" \
    | grep '"tag_name"' | head -1 | sed 's/.*"tag_name"[[:space:]]*:[[:space:]]*"\([^"]*\)".*/\1/'
}

# Download and install a binary
# Args: repo, binary_name, tag, asset_prefix
install_binary() {
  local repo="$1" name="$2" tag="$3" asset_prefix="$4"
  local ext="tar.gz"
  [ "$OS" = "windows" ] && ext="zip"

  local url="https://github.com/${repo}/releases/download/${tag}/${asset_prefix}-${TARGET}.${ext}"
  local tmpdir
  tmpdir=$(mktemp -d)

  info "Downloading ${name} ${tag} for ${TARGET}..."
  dim "${url}"

  if ! curl -fsSL -o "${tmpdir}/archive.${ext}" "$url"; then
    warn "Download failed. Check if release exists for ${TARGET}."
    warn "Fallback: cargo install ${repo##*/}"
    rm -rf "$tmpdir"
    return 1
  fi

  if [ "$ext" = "tar.gz" ]; then
    tar xzf "${tmpdir}/archive.tar.gz" -C "$tmpdir"
  else
    unzip -q "${tmpdir}/archive.zip" -d "$tmpdir"
  fi

  local bin_name="$name"
  [ "$OS" = "windows" ] && bin_name="${name}.exe"

  if [ ! -f "${tmpdir}/${bin_name}" ]; then
    warn "Binary ${bin_name} not found in archive"
    rm -rf "$tmpdir"
    return 1
  fi

  # Install — ensure target dir exists.
  mkdir -p "$INSTALL_DIR"
  if [ -w "$INSTALL_DIR" ]; then
    mv "${tmpdir}/${bin_name}" "${INSTALL_DIR}/${bin_name}"
  else
    info "Need sudo to install to ${INSTALL_DIR}"
    sudo mv "${tmpdir}/${bin_name}" "${INSTALL_DIR}/${bin_name}"
  fi
  [ "$OS" != "windows" ] && chmod +x "${INSTALL_DIR}/${bin_name}"

  rm -rf "$tmpdir"
  info "Installed ${bin_name} → ${INSTALL_DIR}/${bin_name}"
}

main() {
  echo ""
  info "PunkGo Installer"
  echo ""

  detect_platform
  dim "Platform: ${OS}/${ARCH} → ${TARGET}"
  dim "Install dir: ${INSTALL_DIR}"
  echo ""

  # Get latest versions
  local jack_tag kernel_tag
  jack_tag=$(latest_tag "$JACK_REPO") || { warn "Could not fetch latest punkgo-jack release"; exit 1; }
  kernel_tag=$(latest_tag "$KERNEL_REPO") || { warn "Could not fetch latest punkgo-kernel release"; exit 1; }

  # Install both binaries
  # jack asset: punkgo-jack-{TARGET}.tar.gz
  # kernel asset: punkgo-kernel-{tag}-{TARGET}.tar.gz (binary inside: punkgo-kerneld)
  local failed=0
  install_binary "$JACK_REPO" "punkgo-jack" "$jack_tag" "punkgo-jack" || failed=1
  install_binary "$KERNEL_REPO" "punkgo-kerneld" "$kernel_tag" "punkgo-kernel-${kernel_tag}" || failed=1

  echo ""
  if [ "$failed" -eq 0 ]; then
    info "Installation complete!"
    echo ""
    # Check if INSTALL_DIR is in PATH.
    case ":$PATH:" in
      *":${INSTALL_DIR}:"*) ;;
      *)
        warn "${INSTALL_DIR} is not in your PATH."
        if [ "$OS" = "windows" ]; then
          dim "Add it:  setx PATH \"%PATH%;$(cygpath -w "$INSTALL_DIR" 2>/dev/null || echo "$INSTALL_DIR")\""
          dim "Then restart your terminal."
        else
          dim "Add it:  export PATH=\"${INSTALL_DIR}:\$PATH\""
          dim "Or add that line to your ~/.bashrc / ~/.zshrc"
        fi
        echo ""
        ;;
    esac
    dim "Next step:"
    echo "  punkgo-jack setup claude-code"
    echo ""
  else
    warn "Some downloads failed. You can install manually with:"
    echo "  cargo install punkgo-jack"
    echo "  cargo install punkgo-kernel"
    echo ""
  fi
}

main "$@"
