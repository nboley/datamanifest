# Makefile for building and publishing conda packages and Docker images

PACKAGE_NAME := datamanifest
VERSION ?= $(shell grep '^version' pyproject.toml | head -1 | sed -E 's/.*"([^"]+)".*/\1/')
ECR_REGISTRY := 573640641260.dkr.ecr.us-east-1.amazonaws.com/omni
IMAGE_NAME := $(PACKAGE_NAME)
IMAGE_TAG := $(ECR_REGISTRY)/$(IMAGE_NAME):$(VERSION)
IMAGE_LATEST := $(ECR_REGISTRY)/$(IMAGE_NAME):latest

.PHONY: all login conda-login docker-login tag conda conda-build conda-publish docker docker-build docker-push clean

# Main target: login, tag, build conda, build docker, clean
all: login tag conda docker clean

# Verify credentials before building
login: conda-login docker-login

# Check for JFrog credentials (in pip.conf or environment)
conda-login:
	@echo "Checking for JFrog credentials..."
	@HAS_ENV_CREDS=0; \
	HAS_PIP_CREDS=0; \
	if [ -n "$$JFROG_URL" ] && { [ -n "$$JFROG_USER" ] || [ -n "$$JFROG_ACCESS_TOKEN" ]; }; then \
		HAS_ENV_CREDS=1; \
	fi; \
	if ([ -f ~/.config/pip/pip.conf ] && grep -q "index-url.*jfrog" ~/.config/pip/pip.conf 2>/dev/null) || \
		([ -f ~/.pip/pip.conf ] && grep -q "index-url.*jfrog" ~/.pip/pip.conf 2>/dev/null); then \
		HAS_PIP_CREDS=1; \
	fi; \
	if [ $$HAS_ENV_CREDS -eq 0 ] && [ $$HAS_PIP_CREDS -eq 0 ]; then \
		echo "Error: JFrog credentials not found"; \
		echo "Please set JFROG_URL, JFROG_USER/JFROG_PASSWORD or JFROG_ACCESS_TOKEN"; \
		echo "Or configure ~/.config/pip/pip.conf with JFrog index-url"; \
		exit 1; \
	fi; \
	echo "JFrog credentials found"

# Check for AWS CLI (needed for ECR)
docker-login:
	@echo "Checking for AWS CLI..."
	@if ! command -v aws >/dev/null 2>&1; then \
		echo "Error: aws CLI not found"; \
		echo "Please install AWS CLI and configure with: aws configure"; \
		exit 1; \
	fi
	@echo "AWS CLI found"
	@echo "Logging in to ECR..."
	@aws ecr get-login-password --region us-east-1 | docker login --username AWS --password-stdin $(ECR_REGISTRY)

# Create and push git tag
tag:
	@echo "Creating git tag v$(VERSION)..."
	@if git rev-parse "v$(VERSION)" >/dev/null 2>&1; then \
		echo "Error: Tag v$(VERSION) already exists"; \
		echo "Please bump the version in pyproject.toml first"; \
		exit 1; \
	fi
	@git tag -a "v$(VERSION)" -m "Release version $(VERSION)"
	@git push origin "v$(VERSION)"
	@echo "Tagged and pushed v$(VERSION)"

# Build and publish conda package
conda: conda-build conda-publish

# Build conda package with rattler-build
conda-build:
	@echo "Building conda package..."
	@rattler-build build --recipe recipe/recipe.yaml --variant pkg_version=$(VERSION); \
	BUILD_EXIT=$$?; \
	if [ $$BUILD_EXIT -ne 0 ] && [ ! -d output ] || [ -z "$$(find output -name '*.conda' 2>/dev/null)" ]; then \
		echo "Error: Conda build failed (exit code $$BUILD_EXIT)"; \
		exit $$BUILD_EXIT; \
	elif [ $$BUILD_EXIT -ne 0 ]; then \
		echo "Warning: Build succeeded but cleanup failed (exit code $$BUILD_EXIT) - this is a known rattler-build issue"; \
	fi
	@echo "Conda package built"

# Publish conda package to JFrog
conda-publish:
	@echo "Publishing conda package to JFrog..."
	./scripts/publish_conda_package.sh
	@echo "Conda package published"

# Build and push Docker image
docker: docker-build docker-push

# Build Docker image
docker-build:
	@echo "Building Docker image..."
	docker build -t $(IMAGE_TAG) -t $(IMAGE_LATEST) .
	@echo "Docker image built: $(IMAGE_TAG)"

# Push Docker image to ECR
docker-push:
	@echo "Pushing Docker image to ECR..."
	docker push $(IMAGE_TAG)
	docker push $(IMAGE_LATEST)
	@echo "Docker image pushed: $(IMAGE_TAG)"

# Clean build artifacts
clean:
	@echo "Cleaning build artifacts..."
	rm -rf build/ dist/ *.egg-info/ output/ work/ .pytest_cache/
	@echo "Clean complete"
