import pandas as pd
from tqdm.auto import tqdm
from sqlalchemy import create_engine
import pyarrow.parquet as pq

# -----------------------
# Configuración base
# -----------------------
engine = create_engine('postgresql://root:root@localhost:5432/ny_taxi')
parquet_file = 'pipeline/yellow_tripdata_2025-11.parquet'
TABLE_NAME = "yellow_taxi_data"

# Columnas de fechas
parse_dates = [
    "tpep_pickup_datetime",
    "tpep_dropoff_datetime"
]

# -----------------------
# Abrir el parquet
# -----------------------
pf = pq.ParquetFile(parquet_file)
first = True

# -----------------------
# Iterar por row groups (chunks)
# -----------------------
for i in tqdm(range(pf.num_row_groups), desc="Processing row groups"):
    df_chunk = pf.read_row_group(i).to_pandas()
    
    # Convertir columnas de fecha
    for col in parse_dates:
        df_chunk[col] = pd.to_datetime(df_chunk[col])
    
    # Filtrar solo noviembre 2025 (opcional si ya sabes que tu archivo es solo ese mes)
    df_chunk = df_chunk[
        (df_chunk['tpep_pickup_datetime'].dt.year == 2025) &
        (df_chunk['tpep_pickup_datetime'].dt.month == 11)
    ]
    
    if first:
        # Crear esquema de la tabla (sin insertar datos)
        df_chunk.head(0).to_sql(
            name=TABLE_NAME,
            con=engine,
            if_exists="replace",
            index=False
        )
        first = False
        print("Table created")
    
    # Insertar chunk
    df_chunk.to_sql(
        name=TABLE_NAME,
        con=engine,
        if_exists="append",
        index=False
    )
    
    print("Inserted rows:", len(df_chunk))

print("All chunks inserted successfully.")
