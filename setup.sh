#!/usr/bin/env bash
set -euo pipefail

REPO_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"

sudo mkdir -p /data
if ! mountpoint -q /data; then
  # Mount an existing filesystem; disk formatting must be done separately.
  sudo mount /dev/nvme0n1 /data
fi
sudo chmod 777 /data

bash "$REPO_DIR/load_datasets.sh"

curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh

curl -LsSf https://astral.sh/uv/install.sh | sh
source $HOME/.local/bin/env

cd "$REPO_DIR"
uv python install 3.12
uv sync


wget -qO - 'https://proget.makedeb.org/debian-feeds/prebuilt-mpr.pub' | gpg --dearmor | sudo tee /usr/share/keyrings/prebuilt-mpr-archive-keyring.gpg 1> /dev/null
echo "deb [arch=all,$(dpkg --print-architecture) signed-by=/usr/share/keyrings/prebuilt-mpr-archive-keyring.gpg] https://proget.makedeb.org prebuilt-mpr $(lsb_release -cs)" | sudo tee /etc/apt/sources.list.d/prebuilt-mpr.list


sudo apt update
sudo apt install tree default-jre liblog4j2-java cmake numactl tmux

cargo install just

curl -fsSL https://chatgpt.com/codex/install.sh | sh
