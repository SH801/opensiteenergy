#!/bin/bash

# Arguments
MAPPING_FILE=$1
INPUT_OSM=$2
OUTPUT_BASE=$3
ENV_NAME="osm_export_tool"

# 1. Locate and source Conda
CONDA_BASE=$(conda info --base 2>/dev/null)
if [ -z "$CONDA_BASE" ]; then
    # Fallback for common install locations if conda isn't in PATH
    CONDA_BASE="$HOME/miniforge3"
fi

source "$CONDA_BASE/etc/profile.d/conda.sh"

# 2. Activate and Run
conda activate "$ENV_NAME"
osm-export-tool --mapping $MAPPING_FILE $INPUT_OSM $OUTPUT_BASE
