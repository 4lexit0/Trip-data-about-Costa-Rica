import pandas as pd
import numpy as np

trips_raw = pd.read_excel("Trips_Spl_vol2.xlsx", header=None)
ruta = "Energy_Spl_vol2.xlsx"

df = pd.read_excel(ruta, header=8)
df.columns = df.columns.str.strip()
#Renombramos
df = df.rename(columns={
    ".StatusData.Device.DeviceName": "Device",
    "StatusDataDateTime": "Date",
    ".StatusData.Diagnostic.DiagnosticName": "Description",
    ".StatusData.Diagnostic.Source.SourceName": "Source",
    "StatusDataRecordDisplayData": "Value",
    "StatusDataRecordUnitOfMeasureName": "Unit"
})


descs = sorted(df["Description"].dropna().unique())

mapa_desc = {
    "Generic state of charge": "SOC [%]",
    "Electric vehicle battery power": "BatteryPower [W]",
    "Raw odometer": "Odometer [km]",
    "HV battery voltage": "BatteryVoltage [V]",
    "Electric vehicle battery total energy in while driving (since telematics install)": "EnergyInDriving [kWh]",
    "Electric vehicle battery total energy in while idling (since telematics install)": "EnergyInIdling [kWh]",
    "Electric vehicle battery total energy out while driving (since telematics install)": "EnergyOutDriving [kWh]",
    "Electric vehicle battery total energy out while idling (since telematics install)": "EnergyOutIdling [kWh]"
}

df["Variable"] = df["Description"].map(mapa_desc)
df["Variable"].value_counts(dropna=False)

#Ahora filtramos solo las variables válidas
df_sel = df[df["Variable"].notna()].copy()

#Y realizamos el resultado final
df_final = df_sel.pivot_table(
    index=["Device", "Date"],
    columns="Variable",
    values="Value",
    aggfunc="first"
).reset_index()

df_final["BatteryPower [W]"] = df_final["BatteryPower [W]"].fillna(0)

#Ahora, creamos una máscara (solo Power = 0 y Voltage = NaN)
df_final = df_final.sort_values(["Device", "Date"])

df_final = df_final.sort_values(["Device", "Date"])

df_final["BatteryVoltage [V]"] = (
    df_final
    .groupby("Device")["BatteryVoltage [V]"]
    .ffill()
    .fillna(0)
)

df_final["EnergyInDriving [kWh]"] = (
    df_final
    .groupby("Device")["EnergyInDriving [kWh]"]
    .ffill()
    .fillna(0)
)

df_final["EnergyInIdling [kWh]"] = (
    df_final
    .groupby("Device")["EnergyInIdling [kWh]"]
    .ffill()
    .fillna(0)
)

df_final["EnergyOutDriving [kWh]"] = (
    df_final
    .groupby("Device")["EnergyOutDriving [kWh]"]
    .ffill()
    .fillna(0)
)

df_final["EnergyOutIdling [kWh]"] = (
    df_final
    .groupby("Device")["EnergyOutDriving [kWh]"]
    .ffill()
    .fillna(0)
)

df_final["Odometer [km]"] = (
    df_final
    .groupby("Device")["Odometer [km]"]
    .ffill()
    .fillna(0)
)

df_final["SOC [%]"] = (
    df_final
    .groupby("Device")["SOC [%]"]
    .ffill()
    .fillna(0)
)

print(df_final.columns)
df_final.head()

#Ahora vamos a detectar las columnas de "Datos Viajes"
df_trips = pd.read_excel("Trips_Spl_vol2.xlsx", header=10)

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

#Luego, añadimos la columna "TripsDevice" a la tabla final
df_final["Trips Device"] = pd.NA

# 6. Asignar número de viaje
# ===============================
for device, trips_device in df_trips.groupby("DeviceName"):
    mask_device = df_final["Device"] == device

    for _, trip in trips_device.iterrows():
        mask_time = (
            (df_final["Date"] >= trip["TripDetailStartDateTime"]) &
            (df_final["Date"] <= trip["TripDetailStopDateTime"])
        )

        df_final.loc[mask_device & mask_time, "Trips Device"] = trip["DebugTrips"]

#Luego, rellenamos los viajes NaN con el valor del viaje siguiente
df_final["Trips Device"] = (
    df_final.groupby("Device")["Trips Device"]
        .bfill()
)

#Ahora vamos a crear las columnas de tipo de energía consumida por cada viaje
# Columnas de energía originales
energy_cols = [
    "EnergyInDriving [kWh]",
    "EnergyInIdling [kWh]",
    "EnergyOutDriving [kWh]",
    "EnergyOutIdling [kWh]"
]

# Crear nuevas columnas de energía por viaje
for col in energy_cols:
    new_col = f"{col}_Trip"

    df_final[new_col] = (
        df_final
        .groupby(["Device", "Trips Device"])[col]
        .transform(lambda x: x - x.iloc[0])
    )

#Ahora juntaremos todo con el reporte de variables GPS
import pandas as pd

df_gps = pd.read_excel(
    "Collisions_Spl_vol2.xlsx",
    header=9   # fila 10 del Excel
)

df_gps = df_gps.rename(columns={
    "DebugDateTime": "Date",
    "DeviceName" : "Device",
    "DebugLatitude" : "Latitude",
    "DebugLongitude" : "Longitude",
    "DebugSpeed" : "Speed [km/h]"
})

# Asegurar que son numéricos
for col in ["Speed [km/h]", "Latitude", "Longitude"]:
    df_gps[col] = pd.to_numeric(df_gps[col], errors="coerce")

# Eliminar filas sin datos GPS válidos
df_gps = df_gps.dropna(subset=["Speed [km/h]", "Latitude", "Longitude"])


print(df_gps.columns)


df_gps["Date"] = pd.to_datetime(df_gps["Date"])
df_final["Date"] = pd.to_datetime(df_final["Date"])

# Seleccionar columnas necesarias
df_gps = df_gps[
    ["Device", "Date", "Speed [km/h]", "Latitude", "Longitude"]
]

# Orden obligatorio
df_final = df_final.sort_values(["Device", "Date"])
df_gps = df_gps.sort_values(["Device", "Date"])

# Merge temporal
df_final = pd.merge_asof(
    df_final,
    df_gps,
    on="Date",
    by="Device",
    direction="nearest",
)

#Después del último viaje, no se contalizan los viajes, por ende, trip=0
df_final["Trips Device"] = df_final["Trips Device"].fillna(0).astype(int)

#Con ello, volvemos a crrear las columnas de viaje
# Crear nuevas columnas de energía por viaje
for col in energy_cols:
    new_col = f"{col}_Trip"

    df_final[new_col] = (
        df_final
        .groupby(["Device", "Trips Device"])[col]
        .transform(lambda x: x - x.iloc[0])
    )

df_final.count()

df_final.to_csv(
    "tabla_final_1.csv",
    index=False,
    encoding="utf-8-sig"
)
