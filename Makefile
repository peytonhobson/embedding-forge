.PHONY: install install-dev format lint test clean test_llm

install-dev:
	pip install -r requirements-dev.txt

install:
	pip install -e .
	pip install -r requirements.txt

format:
	black .
	isort .

lint:
	flake8 .
	mypy .

test:
	pytest

run: 
	python main.py

clean:
	find . -type d -name "__pycache__" -exec rm -rf {} +
	find . -type f -name "*.pyc" -delete
	find . -type f -name "*.pyo" -delete
	find . -type f -name "*.pyd" -delete
	find . -type d -name "*.egg-info" -exec rm -rf {} +
	find . -type d -name "*.egg" -exec rm -rf {} +
	find . -type d -name ".pytest_cache" -exec rm -rf {} +
	find . -type d -name ".mypy_cache" -exec rm -rf {} +
