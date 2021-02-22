python_setup:
	pyenv install 3.7.7
	pyenv local 3.7.7
	eval "$(pyenv init -)"
	python --version ; pip --version
	python -m pip install pip==20.0.2

clean:
	rm -rf .workspace

install_dependencies:
	echo '>> installing dependencies...'
	eval "$(pyenv init -)"
	pip install -r requirements.txt
	rm -rf ekstep_data_pipelines/binaries
	cp ../lib/wada_snr.tar.gz .
	tar -xzvf wada_snr.tar.gz
	mv WadaSNR ekstep_data_pipelines/binaries
	rm wada_snr.tar.gz

gcloud_setup:
	mkdir -p .workspace
	wget https://dl.google.com/dl/cloudsdk/channels/rapid/downloads/google-cloud-sdk-265.0.0-linux-x86_64.tar.gz -P .workspace
	tar -zxf .workspace/google-cloud-sdk-* --directory .workspace
	./.workspace/google-cloud-sdk/install.sh --quiet
	rm -vf .workspace/google-cloud-sdk-*.tar.gz

gcloud_login:
	./.workspace/google-cloud-sdk/bin/gcloud auth login

setup_cloud_proxy:
	mkdir -p .workspace
	wget https://dl.google.com/cloudsql/cloud_sql_proxy.linux.amd64 -O .workspace/cloud_sql_proxy
	chmod +x ./.workspace/cloud_sql_proxy

start_proxy:
	nohup .workspace/cloud_sql_proxy -dir=./workspace/cloudsql -instances=${GCP_PROJECT}:${GCP_REGION}:${DB_NAME}=tcp:5432 &

mac_setup:
	echo '>> setting up project on mac...'
	brew update
	brew install wget
	brew install pyenv
	brew tap tclass/cloud_sql_proxy && brew install cloud_sql_proxy

	make python_setup
	make install_dependencies
	make apply_dev_policy
	make gcloud_setup
	make gcloud_login


linux_setup:
	echo '>>setting up project..'
	sudo apt-get update
	sudo apt-get install -y ffmpeg
	sudo apt-get install -y sox
	sudo apt-get install -y gcc-multilib g++-multilib
	sudo apt-get install libpq-dev
	sudo apt-get install pyenv
	make python_setup
	make install_dependencies
	make apply_dev_policy
	echo ${$GOOGLE_APPLICATION_CREDENTIALS} > gcp-key.json
	./google-cloud-sdk/bin/gcloud auth activate-service-account --key-file gcp-key.json
	./google-cloud-sdk/bin/gcloud --quiet config set project ${$GOOGLE_APPLICATION_CREDENTIALS}
	make setup_cloud_proxy

apply_dev_policy:
	sh development_policy_setup

format:
	black .

build:
	echo 'building...'

lint:
	find . -type f -name "*.py" | xargs pylint --disable=C0330, C0114, C0116 | cat > reports/lint.txt

test:
	coverage run -m unittest discover -s ekstep_pipelines_tests/ -p "*_tests.py" -v
	coverage report --fail-under 65
	coverage html
	echo 'open htmlcov/index.html'
	open htmlcov/index.html

deploy:
	echo 'deploying...'