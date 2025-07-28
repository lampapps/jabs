#!/usr/bin/bash

echo "Checking for Python 3.12+..."
if command -v python3 &>/dev/null; then
    PYTHON_VERSION=$(python3 -c 'import sys; print(".".join(map(str, sys.version_info[:3])))')
    PYTHON_OK=$(python3 -c 'import sys; print(sys.version_info >= (3,12))')
    if [ "$PYTHON_OK" = "True" ]; then
        echo -e " \xE2\x9C\x93 Python 3.12+ found: $PYTHON_VERSION "
    else
        echo "WARNING: Python version $PYTHON_VERSION found, but 3.12+ is required."
        echo "If Python 3.12 is not installed, you can install it on Ubuntu with:"
        echo "  sudo apt update"
        echo "  sudo apt install python3.12 python3.12-venv"
        echo "Or visit https://www.python.org/downloads/ for other platforms."
        exit 1
    fi
else
    echo -e " \xE2\x9C\x93 Python3 not found."
    exit 1
fi

echo "Checking for python3.12-venv module..."
if python3 -c "import venv" &>/dev/null; then
    echo -e " \xE2\x9C\x93 python3 venv module is available."
else
    echo "WARNING: python3 venv module is missing."
    echo "If Python 3.12 is not installed, you can install it on Ubuntu with:"
    echo "  sudo apt update"
    echo "  sudo apt install python3.12-venv"
    echo "Or visit https://www.python.org/downloads/ for other platforms."
    exit 1
fi

echo "Checking for pip..."
if python3 -m pip --version &>/dev/null; then
    echo -e " \xE2\x9C\x93 pip found."
else
    echo "WARNING: pip not found."
    echo "You can install it on Ubuntuwith:"
    echo "  sudo apt update"
    echo "  sudo apt install python3-pip"
    echo "Or visit https://pip.pypa.io/en/stable/installation/ for other platforms."
    exit 1
fi

echo "Checking for AWS CLI..."
if command -v aws &>/dev/null; then
    echo -e " \xE2\x9C\x93 AWS CLI found."
else
    echo "WARNING: AWS CLI not found."
    echo "If AWS CLI is not installed, you can install it on Ubuntu with:"
    echo "  sudo apt update"
    echo "  sudo apt install awscli"
    echo "Or see https://docs.aws.amazon.com/cli/latest/userguide/getting-started-install.html for other platforms."
fi

echo "Checking for config/global.yaml..."
if [ ! -f config/global.yaml ]; then
    if [ -f config/global-example.yaml ]; then
        echo "config/global.yaml not found. Renaming global-example.yaml to global.yaml."
        if mv config/global-example.yaml config/global.yaml; then
            echo -e " \xE2\x9C\x93 config/global.yaml created from global-example.yaml."
        else
            echo "ERROR: Failed to create config/global.yaml from global-example.yaml."
            echo "Please ensure you have the necessary permissions to rename files in the config directory."
            exit 1
        fi
    else
        echo "Neither config/global.yaml nor config/global-example.yaml found."
        exit 1
    fi
else
    echo -e " \xE2\x9C\x93 config/global.yaml found."
fi

echo "Checking for config/monitor.yaml..."
if [ ! -f config/monitor.yaml ]; then
    if [ -f config/monitor-example.yaml ]; then
        echo "config/monitor.yaml not found. Renaming monitor-example.yaml to monitor.yaml."
        if mv config/monitor-example.yaml config/monitor.yaml; then
            echo -e " \xE2\x9C\x93 config/monitor.yaml created from monitor-example.yaml."
        else
            echo "ERROR: Failed to create config/monitor.yaml from monitor-example.yaml."
            echo "Please ensure you have the necessary permissions to rename files in the config directory."
            exit 1
        fi
    else
        echo "Neither config/monitor.yaml nor config/monitor-example.yaml found."
        exit 1
    fi
else
    echo -e " \xE2\x9C\x93 config/monitor.yaml found."
fi

echo "Setting up virtual environment..."
python3 -m venv venv
source venv/bin/activate

if [ -f requirements.txt ]; then
    echo "Installing requirements..."
    pip install --upgrade pip
    pip install -r requirements.txt
    echo -e " \xE2\x9C\x93 Setup complete."
else
    echo "requirements.txt not found."
    echo "Please ensure you have a requirements.txt file in the current directory."
    exit 1
fi

echo "Running run.py in virtual environment..."
python3 run.py

