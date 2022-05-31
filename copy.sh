#!/bin/bash

sudo scp -i labsuser.pem -r ./GMS $1:~
sudo scp -i labsuser.pem ./load_dataset.sh $1:~
sudo scp -i labsuser.pem ./requirements.txt $1:~
