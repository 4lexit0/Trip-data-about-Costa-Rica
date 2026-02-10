import pandas as pd
import numpy as np

trips_raw = pd.read_excel("Trips_HONDA.xlsx", header=None)
ruta = "Energy_Honda.xlsx"

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
    "Raw odometer": "Odometer [mi]",
    "HV battery voltage": "BatteryVoltage [V]",
    "Electric vehicle battery total energy in while driving (since telematics install)": "Cumulative Energy In Driving [kWh]",
    "Electric vehicle battery total energy in while idling (since telematics install)": "Cumulative Energy In Idling [kWh]",
    "Electric vehicle battery total energy out while driving (since telematics install)": "Cumulative Energy Out Driving [kWh]",
    "Electric vehicle battery total energy out while idling (since telematics install)": "Cumulative Energy Out Idling [kWh]",
    "Electric vehicle battery total energy in during AC charging (since telematics device install)": "Cumulative Energy in AC charging [kWh]",
    "Electric vehicle battery total energy out during AC charging (since telematics device install)": "Cumulative Energy out AC charging [kWh]",
    "Battery maximum cell temperature": "Battery maximum cell temperature [F]",
    "Battery minimum cell temperature": "Battery minimum cell temperature [F]",
    "Hybrid/EV battery temperature": "Thermal state of the battery pack [F]",
    "Electric vehicle battery charge remaining": "Available energy in the battery [F]"
}

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

df_final["Cumulative Energy In Driving [kWh]"] = (
    df_final
    .groupby("Device")["Cumulative Energy In Driving [kWh]"]
    .ffill()
    .bfill()
)

df_final["Cumulative Energy In Idling [kWh]"] = (
    df_final
    .groupby("Device")["Cumulative Energy In Idling [kWh]"]
    .ffill()
    .bfill()
)

df_final["Cumulative Energy Out Driving [kWh]"] = (
    df_final
    .groupby("Device")["Cumulative Energy Out Driving [kWh]"]
    .ffill()
    .bfill()
)

df_final["Cumulative Energy Out Idling [kWh]"] = (
    df_final
    .groupby("Device")["Cumulative Energy Out Idling [kWh]"]
    .ffill()
    .bfill()
)

df_final["Odometer [mi]"] = (
    df_final
    .groupby("Device")["Odometer [mi]"]
    .ffill()
    .bfill()
)

df_final["SOC [%]"] = (
    df_final
    .groupby("Device")["SOC [%]"]
    .ffill()
    .bfill()
)

df_final["Cumulative Energy in AC charging [kWh]"] = (
    df_final
    .groupby("Device")["Cumulative Energy in AC charging [kWh]"]
    .ffill()
    .bfill()
)

df_final["Cumulative Energy out AC charging [kWh]"] = (
    df_final
    .groupby("Device")["Cumulative Energy out AC charging [kWh]"]
    .ffill()
    .bfill()
)

df_final["Battery maximum cell temperature [F]"] = (
    df_final
    .groupby("Device")["Battery maximum cell temperature [F]"]
    .ffill()
    .bfill()
)

df_final["Battery minimum cell temperature [F]"] = (
    df_final
    .groupby("Device")["Battery minimum cell temperature [F]"]
    .ffill()
    .bfill()
)

df_final["Thermal state of the battery pack [F]"] = (
    df_final
    .groupby("Device")["Thermal state of the battery pack [F]"]
    .ffill()
    .bfill()
)

df_final["Available energy in the battery [F]"] = (
    df_final
    .groupby("Device")["Available energy in the battery [F]"]
    .ffill()
    .bfill()
)

print(df_final.columns)
df_final.head()

#Ahora vamos a detectar las columnas de "Datos Viajes"
df_trips = pd.read_excel("Trips_HONDA.xlsx", header=10)

df_trips.columns = df_trips.columns.str.strip()

df_trips = df_trips[
    ["DeviceName", "TripDetailStartDateTime", "TripDetailStopDateTime"]
]

print(df_trips)

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
df_final["Trips Device"] = df_final["Trips Device"].fillna(0).astype(int)

#Ahora vamos a crear las columnas de tipo de energía consumida por cada viaje
# Columnas de energía originales
energy_cols = [
    "Cumulative Energy In Driving [kWh]",
    "Cumulative Energy In Idling [kWh]",
    "Cumulative Energy Out Driving [kWh]",
    "Cumulative Energy Out Idling [kWh]",
    "Cumulative Energy out AC charging [kWh]",
    "Odometer [mi]",
    "Available energy in the battery [F]"
]

df_final["Trip_block"] = (
    df_final.groupby("Device")["Trips Device"]
    .transform(lambda x: x.ne(x.shift()).cumsum())
)

base_col = "Cumulative Energy in AC charging [kWh]"
trip_col = "Cumulative Energy in AC charging [kWh]_Trip"

df_final[trip_col] = (
    df_final
    .groupby(["Device", "Trip_block"])[base_col]
    .transform(lambda x: x - x.iloc[0])
)


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
    "Collisions_HONDA.xlsx",
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

trip_cols = [f"{col}_Trip" for col in energy_cols]

df_final.loc[df_final["Trips Device"] == 0, trip_cols] = 0

#Ahora, Creamos una nueva columna llamada "Charge", la cual vale 1 si el vehículo está cargandose y vale 0 sino.

# Asegurar orden temporal
df_final["Date"] = pd.to_datetime(df_final["Date"])
df_final = df_final.sort_values(["Device", "Date"])

# Diferencias por dispositivo
soc_diff = df_final.groupby("Device")["SOC [%]"].diff()
energy_diff = df_final.groupby("Device")[
    "Cumulative Energy in AC charging [kWh]_Trip"
].diff()

# Lookahead de viaje
trip_next = df_final.groupby("Device")["Trips Device"].shift(-1)

# Inicializar Charge
df_final["Charge"] = 0

# =========================
# REGLAS DE CARGA (1)
# =========================

# SOC sube al menos 0.3 %
mask_soc_up_real = soc_diff >= 0.2

# SOC igual pero energía AC aumenta
mask_energy_up = (soc_diff.abs() < 0.2) & (energy_diff > 0)

df_final.loc[
    mask_soc_up_real | mask_energy_up,
    "Charge"
] = 1

# =========================
# SUAVIZADO DE Charge (fill gaps)
# =========================

charge = df_final["Charge"]

# Ventana pasada (2 anteriores)
prev_2_has_1 = (
    (charge.shift(1) == 1) |
    (charge.shift(2) == 1)
)

# Ventana futura (2 siguientes)
next_2_has_1 = (
    (charge.shift(-1) == 1) |
    (charge.shift(-2) == 1)
)

# Huecos: 0 rodeado de 1s
mask_fill_gap = (
    (charge == 0) &
    prev_2_has_1 &
    next_2_has_1
)

df_final.loc[mask_fill_gap, "Charge"] = 1


# =========================
# REGLAS DURAS (anulan)
# =========================

# En viaje no hay carga
df_final.loc[df_final["Trips Device"] != 0, "Charge"] = 0

# Si el SOC baja, no hay carga
df_final.loc[soc_diff < 0, "Charge"] = 0

# Si cambia de viaje en el siguiente dato, no hay carga
df_final.loc[
    trip_next != df_final["Trips Device"],
    "Charge"
] = 0


#0 rodeado de 1´s = 1
charge = df_final["Charge"]

# 0 rodeado inmediatamente por 1s
mask_surrounded = (
    (charge == 0) &
    (charge.shift(1) == 1) &
    (charge.shift(-1) == 1)
)

df_final.loc[mask_surrounded, "Charge"] = 1

# Tipo entero
df_final.loc[df_final["Trips Device"] != 0, "Charge"] = 0
df_final["Charge"] = df_final["Charge"].astype(int)

df_final_view = df_final.drop(columns=["Trip_block"])

df_final_view.count()

df_final_view.to_csv(
    "tabla_final_HONDA.csv",
    index=False,
    encoding="utf-8-sig"
)
