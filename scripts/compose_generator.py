import sys
import os
import random
import datetime

def generate_client_envs(count: int, env_dir: str = "client/envs"):
    os.makedirs(env_dir, exist_ok=True)

    first_names = ["Juan", "Maria", "Luis", "Ana", "Carlos", "Lucia", "Miguel", "Sofia"]
    last_names = ["Perez", "Gonzalez", "Rodriguez", "Messi", "Ronaldo", "Lopez", "Ramirez", "Sosa"]

    paths = []
    for i in range(1, count + 1):
        env_path = os.path.join(env_dir, f"client{i}.env")
        first = random.choice(first_names)
        last = random.choice(last_names)
        with open(env_path, "w") as ef:
            ef.write(f"CLI_ID={i}\n")
            ef.write(f"CLI_FIRST_NAME={first}\n")
            ef.write(f"CLI_LAST_NAME={last}\n")
            ef.write(f"CLI_DOCUMENT={str(random.randint(10_000_000, 99_999_999))}\n")
            ef.write(f"CLI_BIRTHDATE={str(random.randint(1950, 2005))}-{str(random.randint(1, 12)).zfill(2)}-{str(random.randint(1, 28)).zfill(2)}\n")
            ef.write(f"CLI_NUMBER={str(random.randint(1, 65535))}\n")
        try:
            os.chmod(env_path, 0o644)
        except Exception:
            pass
        paths.append(env_path)
    return paths


if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Uso: python3 compose_generator.py <nombre_archivo_salida> <cantidad_clientes>")
        sys.exit(1)

    nombre_archivo_salida = sys.argv[1]
    cantidad_clientes = int(sys.argv[2])

    generate_client_envs(cantidad_clientes)

    with open(nombre_archivo_salida, 'w') as f:
        f.write("name: tp0\n")
        f.write("services:\n")
        f.write("  server:\n")
        f.write("    container_name: server\n")
        f.write("    image: server:latest\n")
        f.write("    entrypoint: python3 /main.py\n")
        f.write("    environment:\n")
        f.write("      - PYTHONUNBUFFERED=1\n")
        f.write(f"      - EXPECTED_CLIENTS={cantidad_clientes}\n")
        f.write("    networks:\n")
        f.write("      - testing_net\n")
        f.write("    volumes:\n")
        f.write("      - ./server/config.ini:/server/config.ini\n")
        f.write("\n")
        for i in range(1, cantidad_clientes + 1):
            f.write(f"  client{i}:\n")
            f.write(f"    container_name: client{i}\n")
            f.write(f"    image: client:latest\n")
            f.write(f"    entrypoint: /app/client\n")
            f.write(f"    env_file:\n")
            f.write(f"      - ./client/envs/client{i}.env\n")
            f.write(f"    networks:\n")
            f.write(f"      - testing_net\n")
            f.write(f"    volumes:\n")
            f.write(f"      - ./client/config.yaml:/app/config.yaml:ro\n")
            f.write(f"      - .data/agency-{i}.csv:/app/agency.csv:ro\n")
            f.write(f"    depends_on:\n")
            f.write(f"      - server\n")
            f.write("\n")
        f.write("\n")
        f.write("networks:\n")
        f.write("  testing_net:\n")
        f.write("    ipam:\n")
        f.write("      driver: default\n")
        f.write("      config:\n")
        f.write("        - subnet: 172.25.125.0/24\n")