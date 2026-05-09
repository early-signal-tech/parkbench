#!/bin/bash

# parkbench installer script
# Downloads and installs parkbench binary from GitHub releases
# Usage: curl -fsSL https://raw.githubusercontent.com/early-signal-tech/parkbench/main/install.sh | bash
#        curl -fsSL https://raw.githubusercontent.com/early-signal-tech/parkbench/main/install.sh | bash -s v1.0.0
#        curl -fsSL https://raw.githubusercontent.com/early-signal-tech/parkbench/main/install.sh | bash -s latest /usr/local/bin

set -e

# Configuration
VERSION="${1:-latest}"
INSTALL_PATH="${2:-/usr/local/bin}"
BINARY_NAME="parkbench"
GITHUB_REPO="early-signal-tech/parkbench"

# Color output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Detect OS and architecture
detect_platform() {
    OS=$(uname -s)
    ARCH=$(uname -m)
    
    case "$OS" in
        Darwin)
            case "$ARCH" in
                arm64)
                    PLATFORM="darwin-arm64"
                    ;;
                x86_64)
                    PLATFORM="darwin-amd64"
                    ;;
                *)
                    echo -e "${RED}✗ Unsupported macOS architecture: $ARCH${NC}" >&2
                    exit 1
                    ;;
            esac
            ;;
        Linux)
            case "$ARCH" in
                x86_64)
                    PLATFORM="linux-amd64"
                    ;;
                aarch64)
                    PLATFORM="linux-arm64"
                    ;;
                *)
                    echo -e "${RED}✗ Unsupported Linux architecture: $ARCH${NC}" >&2
                    exit 1
                    ;;
            esac
            ;;
        *)
            echo -e "${RED}✗ Unsupported OS: $OS${NC}" >&2
            exit 1
            ;;
    esac
    
    echo -e "${GREEN}✓ Detected platform: $PLATFORM${NC}" >&2
}

# Get latest release version
get_latest_version() {
    if command -v curl &> /dev/null; then
        LATEST=$(curl -s "https://api.github.com/repos/$GITHUB_REPO/releases/latest" | grep '"tag_name"' | cut -d'"' -f4)
        if [ -z "$LATEST" ]; then
            echo -e "${YELLOW}⚠ Could not fetch latest version, using v1.0.0${NC}" >&2
            LATEST="v1.0.0"
        fi
    else
        LATEST="v1.0.0"
    fi
    echo "$LATEST"
}

# Download binary
download_binary() {
    local url="https://github.com/$GITHUB_REPO/releases/download/$VERSION/parkbench-$PLATFORM"
    local tmp_dir=$(mktemp -d)
    local tmp_file="$tmp_dir/parkbench"
    
    echo -e "${YELLOW}⬇ Downloading parkbench $VERSION for $PLATFORM...${NC}" >&2
    
    if ! curl -fsSL "$url" -o "$tmp_file"; then
        echo -e "${RED}✗ Failed to download from $url${NC}" >&2
        rm -rf "$tmp_dir"
        exit 1
    fi
    
    echo "$tmp_file"
}

# Install binary
install_binary() {
    local tmp_file="$1"
    
    # Make it executable
    chmod +x "$tmp_file"
    
    # Check if we need sudo
    if [ -w "$INSTALL_PATH" ]; then
        cp "$tmp_file" "$INSTALL_PATH/$BINARY_NAME"
        echo -e "${GREEN}✓ Installed to $INSTALL_PATH/$BINARY_NAME${NC}" >&2
    else
        if sudo -n true 2>/dev/null; then
            sudo cp "$tmp_file" "$INSTALL_PATH/$BINARY_NAME"
            echo -e "${GREEN}✓ Installed to $INSTALL_PATH/$BINARY_NAME (via sudo)${NC}" >&2
        else
            echo -e "${RED}✗ Permission denied. Need sudo to write to $INSTALL_PATH${NC}" >&2
            exit 1
        fi
    fi
    
    # Cleanup
    rm -rf "$(dirname "$tmp_file")"
}

# Verify installation
verify_installation() {
    if command -v parkbench &> /dev/null; then
        echo -e "${GREEN}✓ Installation verified!${NC}" >&2
        parkbench --help | head -3 >&2
    else
        # Try the install path directly
        if [ -x "$INSTALL_PATH/$BINARY_NAME" ]; then
            echo -e "${GREEN}✓ Installation verified!${NC}" >&2
            "$INSTALL_PATH/$BINARY_NAME" --help | head -3 >&2
        else
            echo -e "${YELLOW}⚠ Binary installed but not in PATH${NC}" >&2
            echo -e "  Add $INSTALL_PATH to your PATH or run: $INSTALL_PATH/$BINARY_NAME${NC}" >&2
        fi
    fi
}

# Main
main() {
    echo -e "${GREEN}parkbench installer${NC}" >&2
    echo "" >&2
    
    # Detect platform
    detect_platform
    
    # Get version
    if [ "$VERSION" = "latest" ]; then
        VERSION=$(get_latest_version)
    fi
    echo -e "${GREEN}✓ Using version: $VERSION${NC}" >&2
    
    # Download
    tmp_file=$(download_binary)
    
    # Install
    install_binary "$tmp_file"
    
    # Verify
    echo "" >&2
    verify_installation
    
    echo "" >&2
    echo -e "${GREEN}✓ parkbench installed successfully!${NC}" >&2
}

main
