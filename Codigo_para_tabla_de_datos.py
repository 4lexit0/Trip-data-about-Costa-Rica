#Para hacer la tabla de datos de GeoTab, debemos de tener el reporte 
#de las colisiones de cada vehículo y el reporte de los viajes que tuvo

#Luego, hay que leer el archivo que contiene la cantidad de viajes
#el inicio y el fin de estos y a qué hora se producieron.
# 1
import pandas as pd
df_trips = pd.read_excel("Datos viajes.xlsx", header=10)

df_trips.columns = df_trips.columns.str.strip()

df_trips = df_trips[
    ["DeviceName", "TripDetailStartDateTime", "TripDetailStopDateTime"]
]

df_trips["TripDetailStartDateTime"] = pd.to_datetime(df_trips["TripDetailStartDateTime"])
df_trips["TripDetailStopDateTime"] = pd.to_datetime(df_trips["TripDetailStopDateTime"])

df_trips = df_trips.sort_values(
    by=["DeviceName", "TripDetailStartDateTime"]
)

# Numerar viajes por vehículo
df_trips["DebugTrips"] = (
    df_trips.groupby("DeviceName").cumcount() + 1
)

#Ahora, hay que leer los datos de las colisiones.

#2
df = pd.read_excel("Datos colisiones.xlsx", header=9)

df.columns = df.columns.str.strip()

df = df[
    [
        "DeviceName",
        "DebugDateTime",
        "DebugRecordType",
        "DebugSpeed",
        "DebugLatitude",
        "DebugLongitude"
    ]
]

df = df[df["DebugRecordType"] == "GpsRecord"]

df["DebugDateTime"] = pd.to_datetime(df["DebugDateTime"])

df = df.sort_values(by=["DeviceName", "DebugDateTime"])

# 3
# Merge cruzado por vehículo
df = df.merge(
    df_trips,
    on="DeviceName",
    how="left"
)

# Quedarse SOLO con puntos dentro de intervalos de viaje
df = df[
    (df["DebugDateTime"] >= df["TripDetailStartDateTime"]) &
    (df["DebugDateTime"] <= df["TripDetailStopDateTime"])
]

# 4
df = df.drop_duplicates(
    subset=["DeviceName", "DebugDateTime"]
)

# 5
df["DebugTimeStamp"] = (
    df.groupby(["DeviceName", "DebugTrips"])["DebugDateTime"]
      .diff()
      .dt.total_seconds()
      .fillna(0)
)


df["DebugNetTimeTrip"] = (
    df.groupby(["DeviceName", "DebugTrips"])["DebugTimeStamp"]
      .cumsum()
)

#Ahora, lo colocamos en un archivo .csv para que podamos ver los resultados.
df.to_csv("viajes_GPS_vol2.csv", index=False)
