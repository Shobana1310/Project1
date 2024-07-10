# setup.sh
#!/bin/bash
set -e  # Exit immediately if a command exits with a non-zero status
export ACCEPT_EULA=Y
export DEBIAN_FRONTEND=noninteractive

apt-get update
apt-get install -y msodbcsql18
