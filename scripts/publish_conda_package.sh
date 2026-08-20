#!/bin/bash
#
# Publish conda packages to JFrog Artifactory
#
# This script uploads rattler-build output artifacts to JFrog Artifactory
# and triggers a metadata reindex for conda package discovery.
#
# Usage:
#   ./scripts/publish_conda_package.sh
#
# Credentials (tried in order):
#   1. Environment variables: ARTIFACTORY_HOST, ARTIFACTORY_USER, ARTIFACTORY_TOKEN
#   2. ~/.pip/pip.conf  extra-index-url  (https://user:token@host/path)
#
# Exit codes:
#   0 - Success
#   1 - Failure (missing credentials, upload failed, etc.)
#
set -euo pipefail

# =============================================================================
# Configuration
# =============================================================================

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"
OUTPUT_DIR="${PROJECT_ROOT}/output"
REPO_NAME="karius-conda"

# =============================================================================
# Helper Functions
# =============================================================================

log_info() {
    echo "[INFO] $*"
}

log_warn() {
    echo "[WARN] $*"
}

log_error() {
    echo "[ERROR] $*" >&2
}

# Parse credentials from ~/.pip/pip.conf if environment variables are not set.
# Expects an extra-index-url in the form: https://username:token@host/path
parse_pip_conf() {
    local pip_conf="${HOME}/.pip/pip.conf"

    if [ ! -f "$pip_conf" ]; then
        return 1
    fi

    # Extract extra-index-url line and parse credentials
    local url
    url=$(grep -E "^\s*extra-index-url\s*=" "$pip_conf" | head -1 | sed 's/.*=\s*//')

    if [ -z "$url" ]; then
        return 1
    fi

    # Parse username:token@host from URL
    if [[ "$url" =~ https://([^:]+):([^@]+)@([^/]+) ]]; then
        PARSED_USER="${BASH_REMATCH[1]}"
        PARSED_TOKEN="${BASH_REMATCH[2]}"
        PARSED_HOST="${BASH_REMATCH[3]}"
        return 0
    fi

    return 1
}

# =============================================================================
# Validation
# =============================================================================

log_info "Validating credentials..."

# Try pip.conf fallback if env vars are not fully set
if [ -z "${ARTIFACTORY_USER:-}" ] || [ -z "${ARTIFACTORY_TOKEN:-}" ]; then
    if parse_pip_conf; then
        log_info "Credentials found in ~/.pip/pip.conf"
        ARTIFACTORY_USER="${ARTIFACTORY_USER:-$PARSED_USER}"
        ARTIFACTORY_TOKEN="${ARTIFACTORY_TOKEN:-$PARSED_TOKEN}"
        ARTIFACTORY_HOST="${ARTIFACTORY_HOST:-$PARSED_HOST}"
    fi
fi

# Default host if still not set
: "${ARTIFACTORY_HOST:=karius.jfrog.io}"

if [ -z "${ARTIFACTORY_USER:-}" ]; then
    log_error "ARTIFACTORY_USER is not set"
    log_error "Set it as an environment variable or add credentials to ~/.pip/pip.conf"
    exit 1
fi

if [ -z "${ARTIFACTORY_TOKEN:-}" ]; then
    log_error "ARTIFACTORY_TOKEN is not set"
    log_error "Set it as an environment variable or add credentials to ~/.pip/pip.conf"
    exit 1
fi

log_info "Using Artifactory host: ${ARTIFACTORY_HOST}"
log_info "Using repository: ${REPO_NAME}"

# =============================================================================
# Locate Artifacts
# =============================================================================

# Read version from pyproject.toml (single source of truth)
VERSION=$(grep '^version' "${PROJECT_ROOT}/pyproject.toml" | head -1 | sed -E 's/.*"([0-9.]+)".*/\1/')

if [ -z "$VERSION" ]; then
    log_error "Could not determine version from pyproject.toml"
    exit 1
fi

log_info "Current version: ${VERSION}"
log_info "Locating build artifacts for version ${VERSION} in ${OUTPUT_DIR}/"

if [ ! -d "${OUTPUT_DIR}" ]; then
    log_error "Build output directory not found: ${OUTPUT_DIR}"
    log_error "Did you run 'make conda-build' first?"
    exit 1
fi

# Collect all .conda and .tar.bz2 files that match the current version.
# Packages are placed under platform subdirectories (e.g. noarch/, linux-64/).
ARTIFACTS=()
while IFS= read -r -d '' file; do
    filename=$(basename "$file")
    if [[ "$filename" == *"-${VERSION}-"* ]]; then
        ARTIFACTS+=("$file")
    else
        log_warn "Skipping ${filename} (does not match version ${VERSION})"
    fi
done < <(find "${OUTPUT_DIR}" -type f \( -name "*.conda" -o -name "*.tar.bz2" \) -print0)

if [ ${#ARTIFACTS[@]} -eq 0 ]; then
    log_error "No conda packages found for version ${VERSION} in ${OUTPUT_DIR}/"
    log_error "Expected files matching pattern: *-${VERSION}-*.conda"
    exit 1
fi

log_info "Found ${#ARTIFACTS[@]} package(s) for version ${VERSION} to upload:"
for artifact in "${ARTIFACTS[@]}"; do
    log_info "  - $(basename "$artifact")"
done

# =============================================================================
# Upload Artifacts
# =============================================================================

log_info "Uploading packages to Artifactory..."

UPLOAD_SUCCESS=0
UPLOAD_SKIPPED=0
UPLOAD_FAILED=0

for artifact in "${ARTIFACTS[@]}"; do
    FILENAME=$(basename "$artifact")
    # Preserve the platform subdirectory (e.g. noarch) in the upload path
    PLATFORM=$(basename "$(dirname "$artifact")")
    UPLOAD_URL="https://${ARTIFACTORY_HOST}/artifactory/${REPO_NAME}/${PLATFORM}/${FILENAME}"

    log_info "Uploading ${FILENAME}..."

    # Capture HTTP status code; disable set -e around curl so we can inspect it
    set +e
    HTTP_CODE=$(curl -L -s -o /dev/null -w "%{http_code}" \
        -u "${ARTIFACTORY_USER}:${ARTIFACTORY_TOKEN}" \
        -T "$artifact" \
        "$UPLOAD_URL" 2>/dev/null)
    CURL_EXIT=$?
    set -e

    if [ $CURL_EXIT -ne 0 ]; then
        log_error "  Upload failed (curl exit code: $CURL_EXIT)"
        UPLOAD_FAILED=$((UPLOAD_FAILED + 1))
        continue
    fi

    case $HTTP_CODE in
        200|201)
            log_info "  Upload successful (HTTP $HTTP_CODE)"
            UPLOAD_SUCCESS=$((UPLOAD_SUCCESS + 1))
            ;;
        409)
            log_warn "  Package already exists in Artifactory (HTTP 409)"
            log_warn "    Bump version in pyproject.toml to publish a new version"
            UPLOAD_SKIPPED=$((UPLOAD_SKIPPED + 1))
            ;;
        *)
            log_error "  Upload failed (HTTP $HTTP_CODE)"
            UPLOAD_FAILED=$((UPLOAD_FAILED + 1))
            ;;
    esac
done

log_info "Upload summary: ${UPLOAD_SUCCESS} succeeded, ${UPLOAD_SKIPPED} skipped (already exist), ${UPLOAD_FAILED} failed"

if [ $UPLOAD_FAILED -gt 0 ]; then
    log_error "Some uploads failed. Aborting."
    exit 1
fi

if [ $UPLOAD_SUCCESS -eq 0 ] && [ $UPLOAD_SKIPPED -gt 0 ]; then
    log_warn "All packages already exist in Artifactory. No new uploads."
fi

# =============================================================================
# Trigger Metadata Reindex
# =============================================================================

log_info "Triggering conda metadata reindex..."

REINDEX_URL="https://${ARTIFACTORY_HOST}/artifactory/api/conda/${REPO_NAME}/reindex?async=true"

HTTP_CODE=$(curl -s -o /dev/null -w "%{http_code}" \
    -X POST \
    -u "${ARTIFACTORY_USER}:${ARTIFACTORY_TOKEN}" \
    "$REINDEX_URL")

case $HTTP_CODE in
    200|201|202)
        log_info "  Reindex triggered successfully (HTTP $HTTP_CODE)"
        log_info "    Metadata reindex is asynchronous and may take a few minutes"
        ;;
    *)
        log_warn "  Reindex request returned HTTP $HTTP_CODE"
        log_warn "    Packages may not be immediately discoverable via 'conda search'"
        log_warn "    Check Artifactory UI or retry reindex manually"
        ;;
esac

# =============================================================================
# Success
# =============================================================================

echo ""
log_info "Publishing complete!"
log_info "Packages are available at:"
log_info "  https://${ARTIFACTORY_HOST}/artifactory/${REPO_NAME}/"
echo ""
log_info "Install with:"
log_info "  conda install -c https://${ARTIFACTORY_HOST}/artifactory/api/conda/${REPO_NAME} datamanifest"
echo ""
