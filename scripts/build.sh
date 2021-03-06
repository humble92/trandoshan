#!/bin/bash

# build processes
docker build . -f build/Dockerfile-crawler -t trandoshan.io/crawler
docker build . -f build/Dockerfile-feeder -t trandoshan.io/feeder
docker build . -f build/Dockerfile-scheduler -t trandoshan.io/scheduler
docker build . -f build/Dockerfile-persister -t trandoshan.io/persister
docker build . -f build/Dockerfile-api -t trandoshan.io/api
