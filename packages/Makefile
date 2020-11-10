tests:
	python -m unittest discover -s ekstep_pipelines_tests/ -p "*_tests.py" -v

util_tests:
	python3 -m unittest discover -s ekstep_pipelines_tests/common/audio_commons/ -p "*_tests.py" -v

trans_client_test:
	python3 -m unittest discover -s ekstep_pipelines_tests/common/audio_commons/transcription_clients_tests -p "*_tests.py" -v


all-tests: tests util_tests trans_client_test
