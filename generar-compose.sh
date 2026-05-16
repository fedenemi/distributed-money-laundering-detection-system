#!/bin/bash

echo "Cantidad de clientes: $1"
python3 docker_compose_generator/compose_generator.py --total_clients $1