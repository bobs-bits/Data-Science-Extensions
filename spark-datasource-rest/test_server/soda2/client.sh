#!/bin/bash

version=9
source=nn
region=Nevada

curl "http://localhost:4567/soda/$version?source=$source&region=$region"

