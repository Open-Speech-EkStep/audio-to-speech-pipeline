python_setup:
	pyenv install 3.7.7
	pyenv local 3.7.7
	eval "$(pyenv init -)"
	python --version ; pip --version

mac_setup:
	brew update
	brew install pyenv

dev_setup:
	python -m pip install pip==20.0.2
	pip install -r requirements.txt
	rm -rf ekstep_data_pipelines/binaries
	cp ../lib/wada_snr.tar.gz . && tar -xzvf wada_snr.tar.gz && mv WadaSNR ekstep_data_pipelines/binaries
	rm wada_snr.tar.gz

apply_dev_policy:
	sh development_policy_setup

format:
	black .

deploy_dags:
	echo 'deploying dags'

tests:
	coverage run -m unittest discover -s ekstep_pipelines_tests/ -p "*_tests.py" -v
	coverage report --fail-under 65
	coverage html
	echo 'open htmlcov/index.html'
	open htmlcov/index.html