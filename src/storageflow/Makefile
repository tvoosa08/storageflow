.DEFAULT_GOAL:= test

include .env

export

local-run:
	PYTHONPATH=../	 poetry run uvicorn storageflow.main:app --reload

test:
	PYTHONPATH=../ poetry run pytest ../../tests -v 

