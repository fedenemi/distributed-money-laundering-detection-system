#!/bin/bash

echo "Cantidad de clientes: $1"
echo "Archivo de transacciones: $2"
echo "Archivo de cuentas: $3"
echo "Carpeta de directorios: $4"
python3 docker_compose_generator/compose_generator.py --total_clients $1 \
            --transactions $2 \
            --accounts $3 \
            --results_dir $4 \