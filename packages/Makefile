python_setup:
	pyenv install 3.7.7
	pyenv local 3.7.7
	eval "$(pyenv init -)"
	python --version ; pip --version
	python -m pip install pip==20.0.2

install_dependencies:
	echo '>> installing dependencies...'
	eval "$(pyenv init -)"
	pip install -r requirements.txt
	rm -rf ekstep_data_pipelines/binaries
	cp ../lib/wada_snr.tar.gz .
	tar -xzvf wada_snr.tar.gz
	mv WadaSNR ekstep_data_pipelines/binaries
	rm wada_snr.tar.gz

mac_setup:
	echo '>> setting up project on mac...'
	brew update
	brew install pyenv
	make python_setup
	make install_dependencies
	make apply_dev_policy


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

apply_dev_policy:
	sh development_policy_setup

format:
	black .

build:
	echo 'building...'

lint:
	find . -type f -name "*.py" | xargs pylint --disable=C0330, C0114, C0116 | cat > reports/lint.txt

tests:
	coverage run -m unittest discover -s ekstep_pipelines_tests/ -p "*_tests.py" -v
	coverage report --fail-under 65
	coverage html
	echo 'open htmlcov/index.html'
	open htmlcov/index.html

deploy:
	echo 'deploying...'


