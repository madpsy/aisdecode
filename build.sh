#!/bin/bash
set -e

# If go.mod does not exist, create one.
if [ ! -f go.mod ]; then
    echo "go.mod not found. Creating go.mod..."
    go mod init aisdecode
fi

# Tidy the module to ensure go.mod and go.sum are up to date.
echo "Tidying module..."
go mod tidy

# Create output directory.
mkdir -p build

# Get host OS and architecture
host_os=$(go env GOOS)
host_arch=$(go env GOARCH)
output_name="aisdecode-${host_os}-${host_arch}"
[[ "$host_os" == "windows" ]] && output_name="${output_name}.exe"

echo "Building for host: ${host_os} (${host_arch})..."
go build -o "build/${output_name}" .

echo "Build succeeded. Binary is located in the 'build' directory."
