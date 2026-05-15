sudo mkdir -p /data
echo '/dev/nvme0n1 /data ext4 defaults 0 0' | sudo tee -a /etc/fstab
sudo mount /data
sudo chmod 777 /data

curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh

curl -LsSf https://astral.sh/uv/install.sh | sh
source $HOME/.local/bin/env

cd madkv
uv python install 3.12
uv sync


wget -qO - 'https://proget.makedeb.org/debian-feeds/prebuilt-mpr.pub' | gpg --dearmor | sudo tee /usr/share/keyrings/prebuilt-mpr-archive-keyring.gpg 1> /dev/null
echo "deb [arch=all,$(dpkg --print-architecture) signed-by=/usr/share/keyrings/prebuilt-mpr-archive-keyring.gpg] https://proget.makedeb.org prebuilt-mpr $(lsb_release -cs)" | sudo tee /etc/apt/sources.list.d/prebuilt-mpr.list


sudo apt update
sudo apt install tree default-jre liblog4j2-java cmake

cargo install just