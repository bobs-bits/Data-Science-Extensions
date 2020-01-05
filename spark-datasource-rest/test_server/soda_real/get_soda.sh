#!/bin/bash

set -x

region=Nevada
source=nn

curl "https://soda.demo.socrata.com/resource/6yvf-kk3n.json?region=${region}&source=$nn"
