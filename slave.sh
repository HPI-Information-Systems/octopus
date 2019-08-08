#!/bin/bash

java -Xms30g -Xmx30g -jar target/octopus-1.0.jar slave -w 20 -mh 172.16.64.61
