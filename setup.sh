#!/bin/bash

# to run this script, save it as setup.sh and execute it with bash setup.sh
# chmod +x setup.sh
# source setup.sh

# Set project root
PROJECT_ROOT=.

# Create directory structure
mkdir -p $PROJECT_ROOT/data
mkdir -p $PROJECT_ROOT/src
mkdir -p $PROJECT_ROOT/tests
mkdir -p $PROJECT_ROOT/logs
mkdir -p $PROJECT_ROOT/.github/workflows

# Create README.md with starter content
cat > $PROJECT_ROOT/README.md <<EOL
# Template Repo

This is a starter template for Python projects.

## Structure

- \`src/\`: Source code
- \`tests/\`: Unit tests
- \`data/\`: Data files
- \`logs/\`: Log files
EOL

# Create pytest.ini with starter content
cat > $PROJECT_ROOT/pytest.ini <<EOL
[pytest]
testpaths = tests # tells which dir to find tests
python_files = test_*.py # tells pytest the prefix fns
pythonpath = src # either root or your folder structure dir

addopts = -v --tb=short
markers =
    slow: marks tests as slow (deselect with '-m "not slow"')
log_cli_level = INFO
filterwarnings =
    ignore::DeprecationWarning
EOL

# Create src/test/test_main.py with starter test code
cat > $PROJECT_ROOT/tests/test_main.py <<EOL
def test_placeholder():
    assert True
EOL

# Create Rotating JSON Logger
cat > $PROJECT_ROOT/src/logger.py <<EOL
import logging
import logging.handlers
import json
import datetime


def format_json(record):
    """Format log record as simplified JSON string"""
    log_entry = {
        "time": datetime.datetime.fromtimestamp(record.created).isoformat(),
        "logger": record.name,
        "level": record.levelname,
        "message": record.getMessage(),
        "line": record.lineno,
    }
    if record.exc_info:
        log_entry["exception"] = logging._defaultFormatter.formatException(
            record.exc_info
        )
    return json.dumps(log_entry)


def setup_logging():
    """Setup logging with simplified JSON format and file rotation"""
    formatter = logging.Formatter()
    formatter.format = format_json

    file_handler = logging.handlers.RotatingFileHandler(
        "./logs/application.log", maxBytes=2 * 1024 * 1024, backupCount=1
    )
    file_handler.setFormatter(formatter)

    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)

    logger = logging.getLogger("json_logger")
    logger.setLevel(logging.INFO)
    if not logger.handlers:
        logger.addHandler(file_handler)
        logger.addHandler(console_handler)

    return logger

# logger = setup_logging()
EOL

# Create GitHub Pull Request Template
cat > $PROJECT_ROOT/.github/pull_request_template.md <<EOL
# Description




## Type of change

- [ ] Bug fix
- [ ] New feature
- [ ] Chore
EOL

# Create GitHub Actions workflow for CI
cat > $PROJECT_ROOT/.github/workflows/ci.yml <<EOL
name: CI Pipeline
on:
  push:
    branches: [ main, develop ]
  pull_request:
    branches: [ main, develop ]
jobs:
  quality:
    name: Code Quality
    runs-on: ubuntu-latest

    steps:
    - name: Checkout code
      uses: actions/checkout@v4

    - name: Set up Python
      uses: actions/setup-python@v4
      with:
        python-version: '3.13'

    - name: Install quality tools
      run: |
        python -m pip install --upgrade pip
        pip install uv

    - name: Check code quality with ruff
      continue-on-error: true
      run: |
        uv run ruff check src/ tests/ --fix

    - name: Test with pytest
      run: |
        uv run pytest tests/ -v --tb=short
EOL

# Create a .env file
cat > $PROJECT_ROOT/.env <<EOL
# Project environment variables
EOL

# Create src/main.py with starter code
cat > $PROJECT_ROOT/src/main.py <<EOL
#!/usr/bin/env python3
import duckdb

duckdb.install_extension("ducklake")
duckdb.load_extension("ducklake")

con = duckdb.connect("my_ducklake.db")
# Attach ducklake to the absolute path of my_ducklake.db & add a (DATA_PATH example/path)
# con.execute("ATTACH 'ducklake:my_ducklake.db' AS my_lake")

con.execute("CREATE SCHEMA IF NOT EXISTS BRONZE")
con.execute("CREATE SCHEMA IF NOT EXISTS SILVER")
con.execute("CREATE SCHEMA IF NOT EXISTS GOLD")

# example query
# with open ("sql/query.sql", "r") as f:
#     query = f.read()
# con.execute(query)

def main():
    print("Welcome to a duckdb pipeline!")

if __name__ == "__main__":
    main()
EOL


# Create .gitignore with common Python ignores
cat > $PROJECT_ROOT/.gitignore <<EOL
# Byte-compiled / optimized / DLL files
__pycache__/
*.py[codz]
*$py.class

# C extensions
*.so

# Distribution / packaging
.Python
build/
develop-eggs/
dist/
downloads/
eggs/
.eggs/
lib/
lib64/
parts/
sdist/
var/
wheels/
share/python-wheels/
*.egg-info/
.installed.cfg
*.egg
MANIFEST

# PyInstaller
#  Usually these files are written by a python script from a template
#  before PyInstaller builds the exe, so as to inject date/other infos into it.
*.manifest
*.spec

# Installer logs
pip-log.txt
pip-delete-this-directory.txt

# Unit test / coverage reports
htmlcov/
.tox/
.nox/
.coverage
.coverage.*
.cache
nosetests.xml
coverage.xml
*.cover
*.py.cover
.hypothesis/
.pytest_cache/
cover/

# Translations
*.mo
*.pot

# Django stuff:
*.log
local_settings.py
db.sqlite3
db.sqlite3-journal

# Flask stuff:
instance/
.webassets-cache

# Scrapy stuff:
.scrapy

# Sphinx documentation
docs/_build/

# PyBuilder
.pybuilder/
target/

# Jupyter Notebook
.ipynb_checkpoints

# IPython
profile_default/
ipython_config.py

# pyenv
#   For a library or package, you might want to ignore these files since the code is
#   intended to run in multiple environments; otherwise, check them in:
# .python-version

# pipenv
#   According to pypa/pipenv#598, it is recommended to include Pipfile.lock in version control.
#   However, in case of collaboration, if having platform-specific dependencies or dependencies
#   having no cross-platform support, pipenv may install dependencies that don't work, or not
#   install all needed dependencies.
#Pipfile.lock

# UV
#   Similar to Pipfile.lock, it is generally recommended to include uv.lock in version control.
#   This is especially recommended for binary packages to ensure reproducibility, and is more
#   commonly ignored for libraries.
uv.lock

# poetry
#   Similar to Pipfile.lock, it is generally recommended to include poetry.lock in version control.
#   This is especially recommended for binary packages to ensure reproducibility, and is more
#   commonly ignored for libraries.
#   https://python-poetry.org/docs/basic-usage/#commit-your-poetrylock-file-to-version-control
#poetry.lock
#poetry.toml

# pdm
#   Similar to Pipfile.lock, it is generally recommended to include pdm.lock in version control.
#   pdm recommends including project-wide configuration in pdm.toml, but excluding .pdm-python.
#   https://pdm-project.org/en/latest/usage/project/#working-with-version-control
#pdm.lock
#pdm.toml
.pdm-python
.pdm-build/

# pixi
#   Similar to Pipfile.lock, it is generally recommended to include pixi.lock in version control.
#pixi.lock
#   Pixi creates a virtual environment in the .pixi directory, just like venv module creates one
#   in the .venv directory. It is recommended not to include this directory in version control.
.pixi

# PEP 582; used by e.g. github.com/David-OConnor/pyflow and github.com/pdm-project/pdm
__pypackages__/

# Celery stuff
celerybeat-schedule
celerybeat.pid

# SageMath parsed files
*.sage.py

# Environments
.env
.envrc
.venv
env/
venv/
ENV/
env.bak/
venv.bak/

# Spyder project settings
.spyderproject
.spyproject

# Rope project settings
.ropeproject

# mkdocs documentation
/site

# mypy
.mypy_cache/
.dmypy.json
dmypy.json

# Pyre type checker
.pyre/

# pytype static type analyzer
.pytype/

# Cython debug symbols
cython_debug/

# PyCharm
#  JetBrains specific template is maintained in a separate JetBrains.gitignore that can
#  be found at https://github.com/github/gitignore/blob/main/Global/JetBrains.gitignore
#  and can be added to the global gitignore or merged into this file.  For a more nuclear
#  option (not recommended) you can uncomment the following to ignore the entire idea folder.
#.idea/

# Abstra
# Abstra is an AI-powered process automation framework.
# Ignore directories containing user credentials, local state, and settings.
# Learn more at https://abstra.io/docs
.abstra/

# Visual Studio Code
#  Visual Studio Code specific template is maintained in a separate VisualStudioCode.gitignore 
#  that can be found at https://github.com/github/gitignore/blob/main/Global/VisualStudioCode.gitignore
#  and can be added to the global gitignore or merged into this file. However, if you prefer, 
#  you could uncomment the following to ignore the entire vscode folder
# .vscode/

# Ruff stuff:
.ruff_cache/

# PyPI configuration file
.pypirc

# Marimo
marimo/_static/
marimo/_lsp/
__marimo__/

# Streamlit
.streamlit/secrets.toml

.DS_Store
EOL

# Create sql/query.sql with starter content
cat > $PROJECT_ROOT/sql/query.sql <<EOL
-- SQL queries go here
-- Example query:
-- SELECT * FROM my_table WHERE condition = 'value';
EOL

# Set up virtual environment & package manager
cd $PROJECT_ROOT
python3 -m uv init

uv add pytest ruff dotenv duckdb polars minio requests prefect

source .venv/bin/activate

git branch -m master main

git add .

git commit -m "Setup Project & Boilerplate"

echo "Setup process completed."
