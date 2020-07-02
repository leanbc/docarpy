
CURRENT_DIR_NAME:=`pwd | xargs basename`

_start_cluster:
	( \
	docker-compose up -d ; \
	)


_stop_cluster:
	( \
	docker-compose stop ; \
	docker-compose rm -f; \
	)

_get_data:
	( \
	docker run -it --rm \
	--name get_data \
	-p 5433:5432 \
	-w /$(CURRENT_DIR_NAME) \
	-v `pwd`:/$(CURRENT_DIR_NAME) \
	--link=docarpy_postgres_1:postgres \
	--net docarpy_default \
	python:3.7.5-slim \
	/bin/bash -c "pip3 install -r ./requirements.txt && python3 ./python_callables/print_results.py --table ${TABLE} --pickup ${PICKUP}  --month ${MONTH} " \
	; \
	)