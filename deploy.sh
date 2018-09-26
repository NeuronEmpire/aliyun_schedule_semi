#!/usr/bin/env bash

SERVER_IP=${SERVER_IP}

cd cmd

GOOS=linux GOARCH=amd64 go build -o test .
scp test root@${SERVER_IP}:~/