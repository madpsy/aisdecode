#!/bin/bash
set -e

# If go.mod does not exist, create one.
if [ ! -f go.mod ]; then
    echo "go.mod not found. Creating go.mod..."
    go mod init alerts
fi

# Tidy the module to ensure go.mod and go.sum are up to date.
echo "Tidying module..."
go mod tidy

# Create output directory.
mkdir -p build

# Build function for a given platform.
build_binary() {
  local os="$1"
  local arch="$2"
  local out="$3"
  echo "Building for ${os} (${arch})..."
  env GOOS="$os" GOARCH="$arch" go build -o "build/${out}" .
}

# Build for each target platform.
build_binary linux   amd64   "alerts-linux-amd64"
build_binary darwin  amd64   "alerts-darwin-amd64"
build_binary windows amd64   "alerts-windows-amd64.exe"
build_binary linux   arm     "alerts-linux-arm"
build_binary linux   arm64   "alerts-linux-arm64"

echo "All builds succeeded. Binaries are located in the 'build' directory."

