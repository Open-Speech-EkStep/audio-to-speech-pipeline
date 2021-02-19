python_setup:
	pyenv install 3.8.6
	pyenv local 3.8.6
	eval "$(pyenv init -)"
	python --version ; pip --version

mac_setup:
	brew update
	brew install pyenv


dev_setup:
	pip install --upgrade pip
	pip install -r requirements.txt
	cp ../lib/wada_snr.tar.gz . && tar -xzvf wada_snr.tar.gz && mv WadaSNR ekstep_data_pipelines/binaries
	rm wada_snr.tar.gz

apply_dev_policy:
	sh development_policy_setup

format:
	black .

deploy_dags:
	echo 'deploying dags'