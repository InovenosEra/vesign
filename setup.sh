#!/bin/bash

echo "Creating virtual environment..."
python3 -m venv .venv

echo "Activating environment..."
source .venv/bin/activate

echo "Installing dependencies..."
pip install --upgrade pip
pip install -r requirements.txt

echo "Setup completed successfully."
echo "To start the system run:"
echo "source .venv/bin/activate"
echo "python main.py"
echo "streamlit run dashboard.py"
