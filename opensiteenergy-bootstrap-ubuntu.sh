#!/bin/bash

# Bootstrap non-interactive install script to run if not using Terraform

export SERVER_USERNAME=admin
export SERVER_PASSWORD=password

echo "SERVER_USERNAME=${SERVER_USERNAME}
SERVER_PASSWORD=${SERVER_PASSWORD}" >> /tmp/.env
sudo apt update -y
sudo apt install wget -y
wget https://raw.githubusercontent.com/SH801/opensite/refs/heads/main/opensiteenergy-build-ubuntu.sh
chmod +x opensiteenergy-build-ubuntu.sh
sudo ./opensiteenergy-build-ubuntu.sh