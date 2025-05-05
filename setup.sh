#!/bin/bash
# filepath: setup.sh

echo "Checking for Python 3.7+..."
if command -v python3 &>/dev/null; then
    PYTHON_VERSION=$(python3 -c 'import sys; print(".".join(map(str, sys.version_info[:3])))')
    PYTHON_OK=$(python3 -c 'import sys; print(sys.version_info >= (3,7))')
    if [ "$PYTHON_OK" = "True" ]; then
        echo "Python 3.7+ found: $PYTHON_VERSION"
    else
        echo "Python version $PYTHON_VERSION found, but 3.7+ is required."
        exit 1
    fi
else
    echo "Python3 not found."
    exit 1
fi

echo "Checking for pip..."
if python3 -m pip --version &>/dev/null; then
    echo "pip found."
else
    echo "pip not found."
    exit 1
fi

echo "Checking for AWS CLI..."
if command -v aws &>/dev/null; then
    echo "AWS CLI found."
else
    echo "AWS CLI not found."
fi

echo "Setting up virtual environment..."
python3 -m venv venv
source venv/bin/activate

if [ -f requirements.txt ]; then
    echo "Installing requirements..."
    pip install --upgrade pip
    pip install -r requirements.txt
    echo "Setup complete."
else
    echo "requirements.txt not found."
    exit 1
fi