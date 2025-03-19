.DEFAULT_GOAL := init

# Initialize the makemarkers directory (helps track out-of-date dependencies).
init:
	mkdir -p .makemarkers


# ----------------------------------------------------------------------------#
#     BUILD STATIC DOCUMENTATION SITE
# ----------------------------------------------------------------------------#

%:
	@echo "dummy target: $@"

.PHONY: html
html: .makemarkers/sphinx-docs


.makemarkers/sphinx-autogen: \
	$(shell find src/op_analytics/datapipeline/schemas -type f -print0 | xargs -0 ls -t | head -n 1) \
	src/op_analytics/cli/subcommands/misc/docsgen/coreschemas.md
	@echo "Running sphinx documentation autegen."
	uv run opdata misc generate_docs
	@touch .makemarkers/sphinx-autogen


.makemarkers/sphinx-docs: \
	.makemarkers/sphinx-autogen \
	$(shell find sphinx -type f -print0 | xargs -0 ls -t | head -n 1)
	$(MAKE) -C sphinx clean
	$(MAKE) -C sphinx html
	@touch .makemarkers/sphinx-docs


# ----------------------------------------------------------------------------#
#     MAKE COPIES OF HTML CONTENT THAT NEEDS TO BE SERVED ON THE STATIC SITE
# ----------------------------------------------------------------------------#

html-copies:
	find "reference_data/market_data/outputs/suggest_base_fee.txt" -type f >> rsync-files.txt
	find "value_locked_flows/img_outputs/html/net_app_flows_7d.html" -type f >> rsync-files.txt
	find "value_locked_flows/img_outputs/html/net_app_flows_30d.html" -type f >> rsync-files.txt
	find "value_locked_flows/img_outputs/html/net_app_flows_90d.html" -type f >> rsync-files.txt
	find "value_locked_flows/img_outputs/html/net_app_flows_365d.html" -type f >> rsync-files.txt
	rsync -aSvuc --recursive --files-from=rsync-files.txt . "docs/"
	rm rsync-files.txt

# ----------------------------------------------------------------------------#
#     LOCAL DEVELOPMENT
# ----------------------------------------------------------------------------#

.PHONY: sphinx-serve
sphinx-serve: .makemarkers/sphinx-docs
	cd docs && uv run python -m http.server


# ----------------------------------------------------------------------------#
#     DOCKER IMAGE
# ----------------------------------------------------------------------------#

IMAGE_TAG = ghcr.io/lithium323/op-analytics:v20250319.1
IMAGE_TAG_DAGSTER = ghcr.io/lithium323/op-analytics-dagster:v20250317.004


.PHONY: uv-build
uv-build:
	rm -rf dummydeps || true
	rm -rf dist || true
	./scripts/python_deps.sh
	uv sync
	uv build

.PHONY: docker-image
docker-image: uv-build
	docker build -f ./Dockerfile --platform linux/amd64 -t ${IMAGE_TAG} .


.PHONY: docker-push
docker-push: docker-image
	docker push ${IMAGE_TAG}


.PHONY: docker-dagster
docker-dagster: uv-build
	docker build -f ./Dockerfile.dagster --platform linux/amd64 -t ${IMAGE_TAG_DAGSTER} .
	docker push ${IMAGE_TAG_DAGSTER}

