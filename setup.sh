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

echo "Checking for config/global.yaml..."
if [ ! -f config/global.yaml ]; then
    if [ -f config/global-example.yaml ]; then
        echo "config/global.yaml not found. Renaming global-example.yaml to global.yaml."
        mv config/global-example.yaml config/global.yaml
    else
        echo "Neither config/global.yaml nor config/global-example.yaml found."
        exit 1
    fi
else
    echo "config/global.yaml found."
fi

echo "Checking for config/monitor.yaml..."
if [ ! -f config/monitor.yaml ]; then
    if [ -f config/monitor-example.yaml ]; then
        echo "config/monitor.yaml not found. Renaming monitor-example.yaml to monitor.yaml."
        mv config/monitor-example.yaml config/monitor.yaml
    else
        echo "Neither config/monitor.yaml nor config/monitor-example.yaml found."
        exit 1
    fi
else
    echo "config/monitor.yaml found."
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

echo "Running run.py in virtual environment..."
python3 run.py

