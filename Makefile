
.DEFAULT_GOAL := test
.PHONY: check-syndicate test test-coverage install install-cli install-modular-cli update-meta clean open-source-executor-image fork-executor-image aws-ecr-login aws-ecr-push syndicate-update-lambdas syndicate-update-api-gateway syndicate-update-meta compose-up compose-down compose-build compose-ps compose-logs


COVERAGE_TYPE := html
DOCKER_EXECUTABLE := podman
CLI_VENV_NAME := venv

# assuming that python is more likely to be installed than jq
AWS_ACCOUNT_ID = $(shell aws sts get-caller-identity | python3 -c "import sys,json;print(json.load(sys.stdin)['Account'])")
AWS_REGION = $(shell aws configure get region)

EXECUTOR_IMAGE_NAME := rule-engine-executor  # just dev image name
EXECUTOR_IMAGE_TAG := latest
SERVER_IMAGE_NAME := public.ecr.aws/x4s4z8e1/syndicate/rule-engine
SERVER_IMAGE_TAG ?= $(shell PYTHONPATH=./src python -B -c "from src.helpers.__version__ import __version__; print(__version__)")

DOCKERFILE_NAME := Dockerfile-opensource-uv
ADDITIONAL_BUILD_PARAMS ?=

SYNDICATE_EXECUTABLE_PATH ?= $(shell which syndicate)
SYNDICATE_CONFIG_PATH ?= .syndicate-config-main
SYNDICATE_BUNDLE_NAME := syndicate-rule-engine

HELM_REPO_NAME := syndicate

DOCKER_COMPOSE ?= docker-compose
COMPOSE_FILE ?= deployment/compose/compose.yaml
COMPOSE_ENV_FILE ?= $(wildcard deployment/compose/.env)
COMPOSE_PROFILES ?= rule-engine modular-api modular-service
# BUILD=1 enables --build (local Dockerfile for rule-engine services)
BUILD ?= 0

COMPOSE_PROFILE_ARGS := $(foreach p,$(COMPOSE_PROFILES),--profile $(p))
COMPOSE_ENV_ARGS := $(if $(COMPOSE_ENV_FILE),--env-file $(firstword $(COMPOSE_ENV_FILE)),)
COMPOSE_BUILD_ARGS := $(if $(filter 1 true yes,$(BUILD)),--build,)

# --- modular-cli ---
MODULAR_CLI_SOURCE ?= pypi
MODULAR_CLI_PACKAGE ?= modular-cli
MODULAR_CLI_GIT_URL ?= git+https://git.epam.com/epmc-eoos/m3-modular-cli.git
MODULAR_CLI_GIT_REF ?= main
MODULAR_CLI_PATH ?= ./modular-cli
MODULAR_CLI_ENTRY_POINT = syndicate
MODULAR_CLI_VENV ?= .modular-cli-venv

check-syndicate:
	@if [[ -z "$(SYNDICATE_EXECUTABLE_PATH)" ]]; then echo "No syndicate executable found"; exit 1; fi
	@if [[ ! -d "$(SYNDICATE_CONFIG_PATH)" ]]; then echo "Syndicate config directory $(SYNDICATE_CONFIG_PATH) not found"; exit 1; fi


test:
	uv run pytest --verbose tests/ cli/srecli_tests/


test-coverage:
	uv run pytest --cov=src/ --cov-report=$(COVERAGE_TYPE) tests/


install:
	@if ! command -v uv >/dev/null 2>&1; then echo "Please, install uv"; exit 1; fi
	uv sync --all-groups --all-extras --prerelease=allow


install-cli:
	# installing CLI in editable mode
	python -m venv $(CLI_VENV_NAME)
	$(CLI_VENV_NAME)/bin/pip install -e ./cli
	@echo "Execute:\nsource ./$(CLI_VENV_NAME)/bin/activate"

install-modular-cli:
	@if ! command -v uv >/dev/null 2>&1; then echo "Please, install uv (https://docs.astral.sh/uv/)"; exit 1; fi
	@if [[ ! -x "$(MODULAR_CLI_VENV)/bin/python" ]]; then uv venv "$(MODULAR_CLI_VENV)"; fi
	@if [[ "$(MODULAR_CLI_SOURCE)" == "pypi" ]]; then \
		uv pip install --python "$(MODULAR_CLI_VENV)/bin/python" "$(MODULAR_CLI_PACKAGE)"; \
		if [[ "$(MODULAR_CLI_ENTRY_POINT)" != "modular-cli" && -x "$(MODULAR_CLI_VENV)/bin/modular-cli" ]]; then \
			ln -sfn modular-cli "$(MODULAR_CLI_VENV)/bin/$(MODULAR_CLI_ENTRY_POINT)"; \
		fi; \
	elif [[ "$(MODULAR_CLI_SOURCE)" == "git" ]]; then \
		MODULAR_CLI_ENTRY_POINT="$(MODULAR_CLI_ENTRY_POINT)" uv pip install --python "$(MODULAR_CLI_VENV)/bin/python" --no-cache "$(MODULAR_CLI_GIT_URL)@$(MODULAR_CLI_GIT_REF)"; \
	elif [[ "$(MODULAR_CLI_SOURCE)" == "path" ]]; then \
		MODULAR_CLI_ENTRY_POINT="$(MODULAR_CLI_ENTRY_POINT)" uv pip install --python "$(MODULAR_CLI_VENV)/bin/python" --no-cache "$(MODULAR_CLI_PATH)"; \
	else \
		echo "Unknown MODULAR_CLI_SOURCE=$(MODULAR_CLI_SOURCE). Use pypi|git|path"; exit 1; \
	fi
	@echo "Installed. Activate and run:"; echo "source ./$(MODULAR_CLI_VENV)/bin/activate"; echo "$(MODULAR_CLI_ENTRY_POINT) --help"

compose-up:
	$(DOCKER_COMPOSE) -f $(COMPOSE_FILE) $(COMPOSE_PROFILE_ARGS) $(COMPOSE_ENV_ARGS) up -d $(COMPOSE_BUILD_ARGS)

compose-down:
	$(DOCKER_COMPOSE) -f $(COMPOSE_FILE) $(COMPOSE_PROFILE_ARGS) $(COMPOSE_ENV_ARGS) down

compose-build:
	$(DOCKER_COMPOSE) -f $(COMPOSE_FILE) $(COMPOSE_PROFILE_ARGS) $(COMPOSE_ENV_ARGS) build

compose-ps:
	$(DOCKER_COMPOSE) -f $(COMPOSE_FILE) $(COMPOSE_PROFILE_ARGS) $(COMPOSE_ENV_ARGS) ps

compose-logs:
	$(DOCKER_COMPOSE) -f $(COMPOSE_FILE) $(COMPOSE_PROFILE_ARGS) $(COMPOSE_ENV_ARGS) logs -f --tail=200

update-meta:
	# updating src/deployment_resources.json (may need to adjust manually after that)
	python src/main.py update_api_models
	# updating src/admin_policy.json
	python src/main.py show_permissions | python -c "import sys,json;json.dump({'customer':'', 'name':'admin_policy','permissions': json.load(sys.stdin)},sys.stdout,indent=2)" > src/admin_policy.json


openapi-spec.json: src/validators/registry.py src/validators/swagger_request_models.py src/validators/swagger_response_models.py src/helpers/constants.py
	python src/main.py generate_openapi > openapi-spec.json


clean:
	-rm -rf .pytest_cache .coverage sre_common_dependencies_layer.zip ./logs htmlcov openapi-spec.json
	-if [[ -d "$(SYNDICATE_CONFIG_PATH)/logs" ]]; then rm -rf "$(SYNDICATE_CONFIG_PATH)/logs"; fi
	-if [[ -d "$(SYNDICATE_CONFIG_PATH)/bundles" ]]; then rm -rf "$(SYNDICATE_CONFIG_PATH)/bundles"; fi


open-source-executor-image:
	$(DOCKER_EXECUTABLE) build -t $(EXECUTOR_IMAGE_NAME):$(EXECUTOR_IMAGE_TAG) -f src/executor/Dockerfile-opensource .


fork-executor-image:
	$(DOCKER_EXECUTABLE) build -t $(EXECUTOR_IMAGE_NAME):$(EXECUTOR_IMAGE_TAG) -f src/executor/Dockerfile .
	# $(DOCKER_EXECUTABLE) build -t $(EXECUTOR_IMAGE_NAME):$(EXECUTOR_IMAGE_TAG) -f src/executor/Dockerfile --build-arg SRE_SERVICE_PATH=custodian-as-a-service --build-arg CLOUD_CUSTODIAN_PATH=custodian-custom-core ..


cli-dist:
	python -m pip install --upgrade build
	python -m build --sdist cli/

obfuscation-manager-dist:
	python -m pip install --upgrade build
	python -m build --sdist obfuscator-cli/

aws-ecr-login:
	@if ! aws --version; then echo "Error: install awscli"; exit 1; fi
	aws ecr get-login-password --region $(AWS_REGION) | $(DOCKER_EXECUTABLE) login --username AWS --password-stdin $(AWS_ACCOUNT_ID).dkr.ecr.$(AWS_REGION).amazonaws.com


aws-ecr-push-executor:
	export AWS_REGION=$(AWS_REGION) AWS_ACCOUNT_ID=$(AWS_ACCOUNT_ID); \
	$(DOCKER_EXECUTABLE) tag $(EXECUTOR_IMAGE_NAME):$(EXECUTOR_IMAGE_TAG) $$AWS_ACCOUNT_ID.dkr.ecr.$$AWS_REGION.amazonaws.com/$(EXECUTOR_IMAGE_NAME):$(EXECUTOR_IMAGE_TAG); \
	$(DOCKER_EXECUTABLE) push $$AWS_ACCOUNT_ID.dkr.ecr.$$AWS_REGION.amazonaws.com/$(EXECUTOR_IMAGE_NAME):$(EXECUTOR_IMAGE_TAG)


syndicate-update-lambdas: check-syndicate
	SDCT_CONF=$(SYNDICATE_CONFIG_PATH) $(SYNDICATE_EXECUTABLE_PATH) build --errors_allowed --bundle_name $(SYNDICATE_BUNDLE_NAME) -F
	SDCT_CONF=$(SYNDICATE_CONFIG_PATH) $(SYNDICATE_EXECUTABLE_PATH) update --update_only_types lambda --update_only_types lambda_layer --bundle_name $(SYNDICATE_BUNDLE_NAME) --replace_output


syndicate-update-meta: check-syndicate
	-rm .$(SYNDICATE_CONFIG_PATH)/bundles/$(SYNDICATE_BUNDLE_NAME)/build_meta.json
	SDCT_CONF=$(SYNDICATE_CONFIG_PATH) $(SYNDICATE_EXECUTABLE_PATH) package_meta -b $(SYNDICATE_BUNDLE_NAME)
	SDCT_CONF=$(SYNDICATE_CONFIG_PATH) $(SYNDICATE_EXECUTABLE_PATH) upload -b $(SYNDICATE_BUNDLE_NAME) -F


syndicate-update-api-gateway: check-syndicate
	# it does not remove the old api gateway
	SDCT_CONF=$(SYNDICATE_CONFIG_PATH) $(SYNDICATE_EXECUTABLE_PATH) deploy --deploy_only_types api_gateway --replace_output --bundle_name $(SYNDICATE_BUNDLE_NAME)


syndicate-update-step-functions: check-syndicate
	# it does not remove the old api gateway
	SDCT_CONF=$(SYNDICATE_CONFIG_PATH) $(SYNDICATE_EXECUTABLE_PATH) deploy --deploy_only_types step_functions --replace_output --bundle_name $(SYNDICATE_BUNDLE_NAME)


#make image-arm64
#make image-amd64
#make push-arm64
#make push-amd64
#make image-manifest
#make push-manifest
image-arm64:
	$(DOCKER_EXECUTABLE) build $(ADDITIONAL_BUILD_PARAMS) --platform linux/arm64 -t $(SERVER_IMAGE_NAME):$(SERVER_IMAGE_TAG)-arm64 -f src/onprem/$(DOCKERFILE_NAME) .

image-amd64:
	$(DOCKER_EXECUTABLE) build $(ADDITIONAL_BUILD_PARAMS) --platform linux/amd64 -t $(SERVER_IMAGE_NAME):$(SERVER_IMAGE_TAG)-amd64 -f src/onprem/$(DOCKERFILE_NAME) .


image-manifest:
	-$(DOCKER_EXECUTABLE) manifest rm $(SERVER_IMAGE_NAME):$(SERVER_IMAGE_TAG)
	$(DOCKER_EXECUTABLE) manifest create $(SERVER_IMAGE_NAME):$(SERVER_IMAGE_TAG) $(SERVER_IMAGE_NAME):$(SERVER_IMAGE_TAG)-arm64 $(SERVER_IMAGE_NAME):$(SERVER_IMAGE_TAG)-amd64
	$(DOCKER_EXECUTABLE) manifest annotate $(SERVER_IMAGE_NAME):$(SERVER_IMAGE_TAG) $(SERVER_IMAGE_NAME):$(SERVER_IMAGE_TAG)-arm64 --arch arm64
	$(DOCKER_EXECUTABLE) manifest annotate $(SERVER_IMAGE_NAME):$(SERVER_IMAGE_TAG) $(SERVER_IMAGE_NAME):$(SERVER_IMAGE_TAG)-amd64 --arch amd64

push-arm64:
	$(DOCKER_EXECUTABLE) push $(SERVER_IMAGE_NAME):$(SERVER_IMAGE_TAG)-arm64


push-amd64:
	$(DOCKER_EXECUTABLE) push $(SERVER_IMAGE_NAME):$(SERVER_IMAGE_TAG)-amd64

push-manifest:
	$(DOCKER_EXECUTABLE) manifest push $(SERVER_IMAGE_NAME):$(SERVER_IMAGE_TAG)


push-helm-chart:
	helm package --dependency-update deployment/helm/rule-engine
	helm s3 push rule-engine-$(SERVER_IMAGE_TAG).tgz $(HELM_REPO_NAME) --relative
	-rm rule-engine-$(SERVER_IMAGE_TAG).tgz
