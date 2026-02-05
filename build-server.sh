#!/bin/bash

./build-cli.sh

# ****************************************************************
# ***** Perform post-build setup specific to server install ******
# ****************************************************************
# Check that processing has finished (PROCESSING has been deleted) then:
# - Set up tileserver-gl as service
# - Change Apache2 settings to point to new tiles
# - Restart tileserver-gl and Apache2

if ! [ -f "PROCESSING" ]; then
    ./build-tileserver-gl.sh
fi
