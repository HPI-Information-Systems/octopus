#!/bin/bash

java -Xms60g -Xmx60g -jar target/octopus-1.0.jar slave -w 20 -mh 172.16.64.61
