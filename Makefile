
.DEFAULT_GOAL := test
.PHONY: check-syndicate test test-coverage install install-cli update-meta clean open-source-executor-image fork-executor-image aws-ecr-login aws-ecr-push syndicate-update-lambdas syndicate-update-api-gateway syndicate-update-meta


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


SYNDICATE_EXECUTABLE_PATH ?= $(shell which syndicate)
SYNDICATE_CONFIG_PATH ?= .syndicate-config-main
SYNDICATE_BUNDLE_NAME := custodian-service

HELM_REPO_NAME := syndicate

check-syndicate:
	@if [[ -z "$(SYNDICATE_EXECUTABLE_PATH)" ]]; then echo "No syndicate executable found"; exit 1; fi
	@if [[ ! -d "$(SYNDICATE_CONFIG_PATH)" ]]; then echo "Syndicate config directory $(SYNDICATE_CONFIG_PATH) not found"; exit 1; fi


test:
	pytest --verbose tests/


test-coverage:
	pytest --cov=src/ --cov-report=$(COVERAGE_TYPE) tests/


install:
	# install for local usage
	@if [[ -z "$(VIRTUAL_ENV)" ]]; then echo "Creating python virtual env"; python -m venv venv; fi
	venv/bin/pip install c7n
	venv/bin/pip install c7n-azure
	venv/bin/pip install c7n-gcp
	venv/bin/pip install c7n-kube
	venv/bin/pip install -r src/onprem/requirements.txt
	venv/bin/pip install -r src/executor/requirements.txt
	@echo "Execute:\nsource ./venv/bin/activate"


install-cli:
	# installing CLI in editable mode
	python -m venv $(CLI_VENV_NAME)
	$(CLI_VENV_NAME)/bin/pip install -e ./cli
	@echo "Execute:\nsource ./$(CLI_VENV_NAME)/bin/activate"


update-meta:
	# updating src/deployment_resources.json (may need to adjust manually after that)
	python src/main.py update_api_models
	# updating src/admin_policy.json
	python src/main.py show_permissions | python -c "import sys,json;json.dump({'customer':'', 'name':'admin_policy','permissions': json.load(sys.stdin)},sys.stdout,indent=2)" > src/admin_policy.json


openapi-spec.json: src/validators/registry.py src/validators/swagger_request_models.py src/validators/swagger_response_models.py src/helpers/constants.py
	python src/main.py generate_openapi > openapi-spec.json


clean:
	-rm -rf .pytest_cache .coverage custodian_common_dependencies_layer.zip ./logs htmlcov openapi-spec.json
	-if [[ -d "$(SYNDICATE_CONFIG_PATH)/logs" ]]; then rm -rf "$(SYNDICATE_CONFIG_PATH)/logs"; fi
	-if [[ -d "$(SYNDICATE_CONFIG_PATH)/bundles" ]]; then rm -rf "$(SYNDICATE_CONFIG_PATH)/bundles"; fi


open-source-executor-image:
	$(DOCKER_EXECUTABLE) build -t $(EXECUTOR_IMAGE_NAME):$(EXECUTOR_IMAGE_TAG) -f src/executor/Dockerfile-opensource .


fork-executor-image:
	$(DOCKER_EXECUTABLE) build -t $(EXECUTOR_IMAGE_NAME):$(EXECUTOR_IMAGE_TAG) -f src/executor/Dockerfile .
	# $(DOCKER_EXECUTABLE) build -t $(EXECUTOR_IMAGE_NAME):$(EXECUTOR_IMAGE_TAG) -f src/executor/Dockerfile --build-arg CUSTODIAN_SERVICE_PATH=custodian-as-a-service --build-arg CLOUD_CUSTODIAN_PATH=custodian-custom-core ..


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


# images with fork which is default for now. Use src/onprem/Dockerfile-opensource for c7n from open source
#make image-arm64
#make image-amd64
#make push-arm64
#make push-amd64
#make image-manifest
#make push-manifest
image-arm64:
	$(DOCKER_EXECUTABLE) build --platform linux/arm64 -t $(SERVER_IMAGE_NAME):$(SERVER_IMAGE_TAG)-arm64 -f src/onprem/Dockerfile .

image-amd64:
	$(DOCKER_EXECUTABLE) build --platform linux/amd64 -t $(SERVER_IMAGE_NAME):$(SERVER_IMAGE_TAG)-amd64 -f src/onprem/Dockerfile .


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
	helm s3 push rule-engine-$(SERVER_IMAGE_TAG).tgz $(HELM_REPO_NAME)
	-rm rule-engine-$(SERVER_IMAGE_TAG).tgz