# [Drone Manager](https://github.com/lacion/drone_manager) Drone Manager based on pyzmq patterns.

***

## Purpose

POC of a drone handling microservice

## Pre Requisites

This project uses ZeroMQ and PyZMQ, for osx run

    brew install zeromq

for ubuntu:

    sudo add-apt-repository ppa:chris-lea/zeromq
    sudo add-apt-repository ppa:chris-lea/libpgm
    sudo apt-get update
    sudo apt-get install libzmq-dev libzmq1

### Python Requirements

development was done using python3, virtualenv and pip to handle dependencies

    virtualenve -p python3 venv
    source venv/bin/activate
    pip install -r requirements.txt

## Running

To run the main Manager simply use:

    python manager.py -H 127.0.0.1:5000 -D 127.0.0.1:5001

for the handler service use:

    python handler.py --manager 127.0.0.1:5000 --id 1

and finally the drone service:

    python drone.py --manager 127.0.0.1:5001 --id 1

you can run as many handlers as wanted, but you can only have 1 drone per handler
or they wont connect
