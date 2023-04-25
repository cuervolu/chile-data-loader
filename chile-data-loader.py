import json
import math
import cx_Oracle
import os
import logging

from tqdm import tqdm
from dotenv import load_dotenv


def load_data_to_db(conn, cursor, data):
    pbar = tqdm(
        total=len(data["regions"]),
        desc="Insertando regiones",
        bar_format="{l_bar}{bar:10}{r_bar}{bar:-10b}",
    )
    for region in data["regions"]:
        pbar.set_postfix(region=region["name"])
        pbar.update()
        pbar.write(f"Insertando región {region['name']}...\n")
        
        # Insertar la región
        try:
            cursor.execute(
                "INSERT INTO region (id_region,nombre, numero_romano) VALUES (:id_region,:nombre, :numero_romano)",
                {
                    "id_region": int(region["number"]),
                    "nombre": region["name"],
                    "numero_romano": region["romanNumber"],
                },
            )

            # Obtener el id_region de la última fila insertada
            cursor.execute(
                "SELECT id_region FROM region WHERE nombre = :nombre",
                {"nombre": region["name"]}
            )
            region_id = cursor.fetchone()[0]

            for comuna in region["communes"]:
                # Insertar la comuna
                cursor.execute(
                    "INSERT INTO comuna (nombre_comuna, region_id) VALUES (:nombre_comuna, :region_id)",
                    {"nombre_comuna": comuna["name"], "region_id": region_id},
                )
                conn.commit()
                pbar.set_postfix(comuna="")
                pbar.write(f"Comuna {comuna['name']} insertada correctamente.\n")

            pbar.set_postfix(comuna="")
            pbar.write(f"Región {region['name']} insertada correctamente.\n")
        except Exception as e:
            logging.error(
                f"No se pudo insertar la región {region['name']}. Error: {str(e)}\n"
            )
            conn.rollback()
            pbar.set_postfix(comuna="")
    pbar.close()


def main():
    # Cargar variables de entorno desde el archivo .env
    load_dotenv()

    # Acceder a variables de entorno
    db_host = os.getenv("DB_HOST")
    db_user = os.getenv("DB_USER")
    db_password = os.getenv("DB_PASSWORD")
    db_port = os.getenv("DB_PORT")
    db_name = os.getenv("DB_NAME")

    # Conectar con la base de datos
    dsn_tns = cx_Oracle.makedsn(db_host, db_port, db_name)
    conn = cx_Oracle.connect(user=db_user, password=db_password, dsn=dsn_tns)
    cursor = conn.cursor()

    # Cargar datos del archivo JSON
    with open("comunas-regiones.json", encoding="UTF-8") as f:
        data = json.load(f)
        # Ordenar la lista de regiones por número en orden ascendente
        data["regions"] = sorted(data["regions"], key=lambda x: int(x["number"]))

    # Cargar datos en la base de datos
    load_data_to_db(conn, cursor, data)

    # Cerrar conexión con la base de datos
    cursor.close()
    conn.close()
    print("Proceso completado.")


if __name__ == "__main__":
    main()
