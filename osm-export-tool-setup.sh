#!/bin/bash

# --- Configuration ---
ENV_NAME="osm_export_tool"
PYTHON_VERSION="3.9"
GDAL_VERSION="3.8"
INSTALL_PATH="$HOME/miniforge3"

# --- 1. Universal Miniforge Installer ---
if ! command -v conda &> /dev/null; then
    echo "Conda not found. Starting universal Miniforge installation..."
    
    OS_TYPE=$(uname -s)
    ARCH_TYPE=$(uname -m)
    
    if [ "$OS_TYPE" == "Darwin" ]; then
        INSTALLER="Miniforge3-MacOSX-${ARCH_TYPE}.sh"
    elif [ "$OS_TYPE" == "Linux" ]; then
        INSTALLER="Miniforge3-Linux-${ARCH_TYPE}.sh"
    else
        echo "Error: Unsupported OS: $OS_TYPE"
        exit 1
    fi

    URL="https://github.com/conda-forge/miniforge/releases/latest/download/${INSTALLER}"
    
    echo "Downloading installer for ${OS_TYPE} ${ARCH_TYPE}..."
    curl -L -O "$URL"
    
    echo "Running batch installation to ${INSTALL_PATH}..."
    bash "$INSTALLER" -b -p "$INSTALL_PATH"
    rm "$INSTALLER"

    CONDA_BASE="$INSTALL_PATH"
else
    CONDA_BASE=$(conda info --base)
fi

# --- 2. Initialize Conda for Script Usage ---
source "${CONDA_BASE}/etc/profile.d/conda.sh"

# --- 3. Quick Exit Check ---
ENV_PATH="${CONDA_BASE}/envs/$ENV_NAME"
SENTINEL_FILE="$ENV_PATH/.setup_done"

if [ -f "$SENTINEL_FILE" ]; then
    echo "Environment '$ENV_NAME' is already configured. Fast-quitting..."
    exit 0
fi

# --- 4. Environment Creation ---
if conda info --envs | grep -q "$ENV_NAME"; then
    echo "Environment '$ENV_NAME' exists but sentinel is missing. Repairing..."
else
    echo "Creating conda environment '$ENV_NAME' with GDAL..."
    conda create -n "$ENV_NAME" -c conda-forge python=$PYTHON_VERSION gdal=$GDAL_VERSION -y
fi

# --- 5. Activation and Pip Installs ---
echo "Activating '$ENV_NAME' and installing dependencies..."
conda activate "$ENV_NAME"

echo "Using $(python --version) at $(which python)"

pip install --upgrade pip
pip install osmium~=3.5.0 pyparsing~=2.4.0 pyyaml~=5.1.1 requests~=2.26.0 landez~=2.5.0
pip install shapely~=1.6 
pip install git+https://github.com/hotosm/osm-export-tool-python --no-deps

# --- 6. Final Validation & Flagging ---
if [ -f "${ENV_PATH}/bin/osm-export-tool" ]; then
    echo "Validation successful."
    touch "$SENTINEL_FILE"
    echo "Environment is ready."
else
    echo "Error: 'osm-export-tool' binary not found in ${ENV_PATH}/bin/"
    exit 1
fi

echo "Setup Complete!"