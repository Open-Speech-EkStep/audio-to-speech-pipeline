#!/usr/bin/env bash

echo "Downloading talisman"
curl https://thoughtworks.github.io/talisman/install.sh > ~/install-talisman.sh
chmod +x ~/install-talisman.sh
echo "Installing talisman"
~/install-talisman.sh
echo "Installation complete"
