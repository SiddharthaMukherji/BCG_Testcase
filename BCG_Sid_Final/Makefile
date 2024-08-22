# Define make help functionality
.DEFAULT_GOAL := help
define PRINT_HELP_PYSCRIPT
import re, sys
for line in sys.stdin:
	match = re.match(r'^([a-zA-Z_-]+):.*?## (.*)$$', line)
	if match:
	   target, help = match.groups()
	   print("%-20s %s" % (target, help))
endef
export PRINT_HELP_PYSCRIPT

help: ## Get list of make commands to build and run this job
	@printf -- "Make commands for BCG case Study\n"
	@python3 -c "$$PRINT_HELP_PYSCRIPT" < $(MAKEFILE_LIST)

prep_to_run: ## Unzip the data
	unzip Data.zip -d . -x "__MACOSX*"

clean: ## Remove generated files and directories
	rm -rf build dist *.egg-info

build: clean ## Build the Python package (bcg)
	python3 setup.py bdist_egg

install: build ## Install the built package into your environment
	pip3 install .

run: install ## Run the main application (assuming main.py)
	spark-submit \
		--master local[*] \
		--name "Vehicle Accident Analysis" \
		main.py \
		[--py-files dist/*.egg]

.PHONY: help prep_to_run clean build install run