INDEX=bulkprocessor-test
TYPE=record

default: build

build:
	go build

build-race:
	go build -race

run:
	./bulkprocessor-test -index=$(INDEX) -type=$(TYPE)

count:
	curl -s -XGET 'http://localhost:9200/$(INDEX)/$(TYPE)/_count?pretty'

drop:
	curl -s -XDELETE 'http://localhost:9200/$(INDEX)?pretty'
