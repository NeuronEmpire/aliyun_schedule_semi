#!/usr/bin/env bash

cd schedule
go build
mv schedule ../test
cd ../
./test