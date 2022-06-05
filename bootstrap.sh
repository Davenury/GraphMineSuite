#!/bin/sh

pip3 install autopep8
pip3 install nose
pip3 install numpy
pip3 install pandas --no-warn-script-location
pip3 install plotly
pip3 install py4j
pip3 install pycodestyle
pip3 install pyroaring
pip3 install pyspark==2.4 --no-warn-script-location
pip3 install python-dateutil
pip3 install pytz
pip3 install six
pip3 install tenacity
pip3 install toml
pip3 install boto3

path=/home/hadoop

sudo aws s3 cp --recursive s3://gms-us-east-1/GMS $path
sudo aws s3 cp --recursive s3://gms-us-east-1/twitter $path/twitter
