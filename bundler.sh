#!/bin/bash

set -e  # Exit on errors
VERSION="v1.0.1BETA"

echo "BUILDING SUPERMASSIVE BINARIES"

PLATFORMS=(
  "darwin amd64"
  "darwin arm64"
  "linux 386"
  "linux amd64"
  "linux arm"
  "linux arm64"
  "freebsd arm"
  "freebsd amd64"
  "freebsd 386"
  "windows amd64"
  "windows arm64"
  "windows 386"
)

for platform in "${PLATFORMS[@]}"; do
  read -r GOOS GOARCH <<< "$platform"

  OUTPUT_DIR="bin/${GOOS}/${GOARCH}"
  OUTPUT_FILE="supermassive"
  EXT=""
  ARCHIVE_EXT=".tar.gz"

  mkdir -p "$OUTPUT_DIR"

  # Windows builds need .exe and zip archives
  if [[ "$GOOS" == "windows" ]]; then
    EXT=".exe"
    ARCHIVE_EXT=".zip"
  fi

  echo "Building for $GOOS/$GOARCH..."

  # Build command
  (cd src && GOOS="$GOOS" GOARCH="$GOARCH" go build -o "../$OUTPUT_DIR/$OUTPUT_FILE$EXT")

  # Archive the build
  if [[ "$GOOS" == "windows" ]]; then
    zip -j "bin/${GOOS}/${GOARCH}/supermassive-${VERSION}-${GOARCH}.zip" "$OUTPUT_DIR/$OUTPUT_FILE$EXT"
  else
    tar -czf "bin/${GOOS}/${GOARCH}/supermassive-${VERSION}-${GOARCH}.tar.gz" -C "$OUTPUT_DIR" "$OUTPUT_FILE$EXT"
  fi
done

echo "DONE!"
