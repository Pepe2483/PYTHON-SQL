# Python Importar excel Partos hasta servicio .Xlsm

# 1)Partos

```python
import os
import urllib.parse
import pandas as pd
import numpy as np
from sqlalchemy import create_engine, text

# ========= CONFIG =========
SERVER   = "DESKTOP-3I6O45D"
DATABASE = "EVALUACION_ESTABLOS"
USER     = "sa"
PASSWORD = "123456789"

BASE_PATH = r"D:\PRUEBA PYTHON"
ESTABLOS = [
    "AGHOLSTEIN","ANGUS","ANTONIOJAIME","BALI","CANELAS","ESTRELLADEMANUEL",
    "GASUR","HUERTOS SAN MARTIN","LAREDO","MOCHERA","PIAMONTE","REMANSO",
    "SANTA FE","SAUSALITO","SEQUION"
]

# === ORDEN EXACTO DE COLUMNAS A INSERTAR (SIN IDENTITY) ===
COLUMNAS_SQL = [
    "Source.Name","DairyName","TODAY","cow","lc","dim","stat","bd","typ",
    "ClvVAIdCode","ClvNumLact","ClvCtrlCode","ClvOffC","ClvCom","ClvCom2",
    "ClvCount","ClvCntLf","ClvCost","ClvRevCode","ClvDim","ClvAge","ClvTech",
    "ClvClvEase","ClvClvEaseCod","ClvDat","ClvTime","Clv2Do","ClvSidEffL2",
    "ClvDiag","Source.FileDate","Source.FileTime"
]

# ========= CONEXIÓN =========
def _make_engine(driver_name: str, encrypt_yes: bool):
    conn = (
        f"DRIVER={{{driver_name}}};SERVER={SERVER};DATABASE={DATABASE};UID={USER};PWD={PASSWORD};"
        f"TrustServerCertificate=Yes;Encrypt={'Yes' if encrypt_yes else 'No'};"
    )
    return create_engine(
        "mssql+pyodbc:///?odbc_connect=" + urllib.parse.quote_plus(conn),
        fast_executemany=True, pool_pre_ping=True
    )

def get_engine():
    for drv, enc in [("ODBC Driver 18 for SQL Server", True),
                     ("ODBC Driver 17 for SQL Server", False)]:
        try:
            eng = _make_engine(drv, enc)
            with eng.connect():
                pass
            print(f"Conectado con {drv}")
            return eng
        except Exception as e:
            print(f"No se pudo con {drv}: {e}")
    raise RuntimeError("No se pudo abrir conexión ODBC 17/18.")

ENGINE = get_engine()

# ========= NORMALIZACIONES =========
def to_date_series(s: pd.Series) -> pd.Series:
    return pd.to_datetime(s, errors="coerce").dt.date

def excel_days_to_time_from_series(s: pd.Series) -> pd.Series:
    # Excel almacena tiempos como fracción de día; 1899-12-30 base
    td = pd.to_timedelta(pd.to_numeric(s, errors="coerce"), unit="D")
    t = (pd.Timestamp("1899-12-30") + td).dt.time
    return t

def format_time_series_to_string(tser: pd.Series) -> pd.Series:
    # Devuelve 'HH:MM:SS.mmm' o None
    def fmt(x):
        if pd.isna(x):
            return None
        try:
            return f"{x.strftime('%H:%M:%S.%f')[:-3]}"
        except Exception:
            return None
    return tser.apply(fmt)

def normalize_clvtime(col: pd.Series) -> pd.Series:
    s = col.copy()
    if pd.api.types.is_timedelta64_dtype(s):
        t = (pd.Timestamp("1900-01-01") + s).dt.time
        return format_time_series_to_string(t)
    if pd.api.types.is_numeric_dtype(s):
        t = excel_days_to_time_from_series(s)
        return format_time_series_to_string(t)
    # intentar parsear strings y datetimes
    t = pd.to_datetime(s, errors="coerce").dt.time
    return format_time_series_to_string(t)

def normalize_dataframe(df: pd.DataFrame, establo: str) -> pd.DataFrame:
    # Asegura columnas y orden
    for c in COLUMNAS_SQL:
        if c not in df.columns:
            df[c] = np.nan
    df = df[COLUMNAS_SQL]

    # Defaults
    df["DairyName"] = df["DairyName"].fillna(establo)

    # Fechas
    for c in ["TODAY","bd","ClvDat","Clv2Do","Source.FileDate"]:
        df[c] = to_date_series(df[c])

    # Hora como VARCHAR(64)
    df["ClvTime"] = normalize_clvtime(df["ClvTime"])

    # Texto
    varchar_cols = ["Source.Name","DairyName","cow","stat","typ","ClvVAIdCode",
                    "ClvCtrlCode","ClvOffC","ClvCom","ClvCom2","ClvRevCode",
                    "ClvTech","ClvClvEase","ClvClvEaseCod","ClvDiag","ClvSidEffL2",
                    "Source.FileTime","ClvTime"]
    for c in varchar_cols:
        df[c] = df[c].astype(str).where(df[c].notna(), None)

    # cow puede venir numérica: convertir a texto sin .0
    df["cow"] = df["cow"].apply(lambda x: None if pd.isna(x) else str(int(x)) if isinstance(x,(int,np.integer)) or (isinstance(x,float) and x.is_integer()) else str(x))

    # Números
    for c in ["lc","dim","ClvNumLact","ClvCount","ClvCntLf","ClvDim"]:
        df[c] = pd.to_numeric(df[c], errors="coerce").astype("Int64")
    df["ClvCost"] = pd.to_numeric(df["ClvCost"], errors="coerce")
    df["ClvAge"]  = pd.to_numeric(df["ClvAge"],  errors="coerce")

    return df

# ========= UTILIDADES =========
def ts_str(dt: pd.Timestamp) -> str:
    ms3 = dt.microsecond // 1000
    return f"{dt.strftime('%Y-%m-%d %H:%M:%S')}.{ms3:03d}"

# ========= ÍNDICE SQL + ESCANEO FS =========
def cargar_indice_existente() -> dict:
    try:
        q = """
        SELECT [Source.Name] AS SName,
               CONVERT(varchar(10), [Source.FileDate], 120) + ' ' + CONVERT(varchar(12), [Source.FileTime]) AS STS
        FROM dbo.PARTOS
        WHERE [Source.Name] IS NOT NULL AND [Source.FileDate] IS NOT NULL AND [Source.FileTime] IS NOT NULL
        GROUP BY [Source.Name], [Source.FileDate], [Source.FileTime]
        """
        df = pd.read_sql_query(q, ENGINE)
        idx = (df.assign(dt=pd.to_datetime(df["STS"], errors="coerce"))
                 .sort_values(["SName","dt"])
                 .dropna(subset=["dt"])
                 .groupby("SName")["STS"].last()
                 .to_dict())
        return idx
    except Exception as e:
        print(f"No se pudo leer índice existente: {e}")
        return {}

def escanear_archivos_fs() -> set:
    presentes = set()
    for establo in ESTABLOS:
        carpeta = os.path.join(BASE_PATH, establo, "PARTOS")
        if not os.path.isdir(carpeta):
            continue
        for f in os.listdir(carpeta):
            if f.lower().endswith(".xlsx") and not f.startswith("~$"):
                presentes.add(f)
    return presentes

# ========= BORRADO POR ARCHIVOS ELIMINADOS =========
def borrar_faltantes_en_fs():
    indice = cargar_indice_existente()
    en_sql = set(indice.keys())
    en_fs  = escanear_archivos_fs()
    faltantes = sorted(en_sql - en_fs)
    if not faltantes:
        print("✔ No hay archivos eliminados en FS pendientes de borrar en SQL.")
        return
    print(f"🧹 Borrando {len(faltantes)} archivos inexistentes en FS…")
    with ENGINE.begin() as con:
        for nm in faltantes:
            con.execute(text("DELETE FROM dbo.PARTOS WHERE [Source.Name] = :nm"), {"nm": nm})
            print(f"   - eliminado en SQL: {nm}")

# ========= CARGA INCREMENTAL =========
def cargar_establos_incremental():
    indice = cargar_indice_existente()
    columnas_excel = [c for c in COLUMNAS_SQL if c not in {"Source.Name","Source.FileDate","Source.FileTime"}]

    for establo in ESTABLOS:
        carpeta = os.path.join(BASE_PATH, establo, "PARTOS")
        if not os.path.isdir(carpeta):
            continue

        archivos = [f for f in os.listdir(carpeta) if f.lower().endswith(".xlsx") and not f.startswith("~$")]

        for archivo in archivos:
            ruta = os.path.join(carpeta, archivo)
            try:
                mtime = pd.to_datetime(os.path.getmtime(ruta), unit="s")
                actual_ts = ts_str(mtime)
                ts_prev = indice.get(archivo)

                if ts_prev is not None and pd.to_datetime(actual_ts) <= pd.to_datetime(ts_prev):
                    print(f"⏩ Saltando {archivo} (sin cambios, mtime {actual_ts})")
                    continue

                if ts_prev is not None and pd.to_datetime(actual_ts) > pd.to_datetime(ts_prev):
                    with ENGINE.begin() as con:
                        con.execute(text("DELETE FROM dbo.PARTOS WHERE [Source.Name] = :nm"), {"nm": archivo})
                    print(f"🧹 Eliminado previo de {archivo} (TS previo {ts_prev})")

                # Leer Excel sin encabezado. Los nombres reales los imponemos con columnas_excel
                df = pd.read_excel(ruta, header=None, names=columnas_excel, engine="openpyxl")
                df.dropna(how="all", inplace=True)
                if df.empty:
                    print(f"⚠ {archivo} vacío, omitido.")
                    continue

                # Metadatos de origen
                df["Source.Name"]     = archivo
                df["Source.FileDate"] = mtime.date()
                df["Source.FileTime"] = actual_ts.split(" ")[1]

                # Normalizar y completar
                df = normalize_dataframe(df, establo)

                print(f"⏳ Cargando {archivo} desde {establo} ({actual_ts})…")
                df.to_sql("PARTOS", ENGINE, if_exists="append", index=False, chunksize=1000, schema="dbo")
                print(f"✅ Cargado {archivo}.\n")

                indice[archivo] = actual_ts

            except Exception as e:
                print(f"❌ Error al cargar {archivo} desde {establo}: {e}\n")

if __name__ == "__main__":
    borrar_faltantes_en_fs()
    cargar_establos_incremental()

```

# 2)ABORTOS

```python
import os
import urllib.parse
import pandas as pd
import numpy as np
from sqlalchemy import create_engine, text

# ========= CONFIG =========
SERVER   = "DESKTOP-3I6O45D"
DATABASE = "EVALUACION_ESTABLOS"
USER     = "sa"
PASSWORD = "123456789"

BASE_PATH = r"D:\PRUEBA PYTHON"
ESTABLOS = [
    "AGHOLSTEIN","ANGUS","ANTONIOJAIME","BALI","CANELAS","ESTRELLADEMANUEL",
    "GASUR","HUERTOS SAN MARTIN","LAREDO","MOCHERA","PIAMONTE","REMANSO",
    "SANTA FE","SAUSALITO","SEQUION"
]
c
# === ORDEN EXACTO DE COLUMNAS A INSERTAR (SIN IDENTITY) ===
COLUMNAS_SQL = [
    "Source.Name","DairyName","TODAY","COW","STAT","BD","TYP",
    "AbtVAIdCode","AbtNumLact","AbtCtrlCode","AbtOffC","AbtCom","AbtCom2",
    "AbtCount","AbtCntLf","AbtCost","AbtDim","AbtAge","AbtTech",
    "AbtClvEase","AbtClvEaseCod","AbtDat","AbtTime","AbtSidEffL2","AbtDiag",
    "FECHA_DE_PARTO","Source.FileDate","Source.FileTime"
]

# ========= CONEXIÓN =========
def _make_engine(driver_name: str, encrypt_yes: bool):
    conn = (
        f"DRIVER={{{driver_name}}};SERVER={SERVER};DATABASE={DATABASE};UID={USER};PWD={PASSWORD};"
        f"TrustServerCertificate=Yes;Encrypt={'Yes' if encrypt_yes else 'No'};"
    )
    return create_engine(
        "mssql+pyodbc:///?odbc_connect=" + urllib.parse.quote_plus(conn),
        fast_executemany=True, pool_pre_ping=True
    )

def get_engine():
    for drv, enc in [("ODBC Driver 18 for SQL Server", True),
                     ("ODBC Driver 17 for SQL Server", False)]:
        try:
            eng = _make_engine(drv, enc)
            with eng.connect():
                pass
            print(f"Conectado con {drv}")
            return eng
        except Exception as e:
            print(f"No se pudo con {drv}: {e}")
    raise RuntimeError("No se pudo abrir conexión ODBC 17/18.")

ENGINE = get_engine()

# ========= NORMALIZACIONES =========
def to_date_series(s: pd.Series) -> pd.Series:
    return pd.to_datetime(s, errors="coerce", dayfirst=True).dt.date

def excel_days_to_time_from_series(s: pd.Series) -> pd.Series:
    td = pd.to_timedelta(pd.to_numeric(s, errors="coerce"), unit="D")
    return (pd.Timestamp("1899-12-30") + td).dt.time

def format_time_hms_series(tser: pd.Series) -> pd.Series:
    def fmt(x):
        if pd.isna(x):
            return None
        try:
            return x.strftime("%H:%M:%S")
        except Exception:
            return None
    return tser.apply(fmt)

def normalize_time(col: pd.Series) -> pd.Series:
    s = col.copy()
    if pd.api.types.is_timedelta64_dtype(s):
        t = (pd.Timestamp("1900-01-01") + s).dt.time
        return format_time_hms_series(t)
    if pd.api.types.is_numeric_dtype(s):
        t = excel_days_to_time_from_series(s)
        return format_time_hms_series(t)
    t = pd.to_datetime(s, errors="coerce").dt.time
    return format_time_hms_series(t)

def to_int_nullable(series: pd.Series) -> pd.Series:
    x = pd.to_numeric(series, errors="coerce")
    # si trae decimales en columnas enteras, se colocan como NA
    x = x.where(x.isna() | (np.mod(x, 1) == 0))
    return x.astype("Int64")

def normalize_dataframe(df: pd.DataFrame, establo: str) -> pd.DataFrame:
    # columnas y orden
    for c in COLUMNAS_SQL:
        if c not in df.columns:
            df[c] = np.nan
    df = df[COLUMNAS_SQL]

    # defaults
    df["DairyName"] = df["DairyName"].fillna(establo)

    # fechas
    for c in ["TODAY","AbtDat","FECHA_DE_PARTO","Source.FileDate"]:
        df[c] = to_date_series(df[c])

    # hora
    df["AbtTime"] = normalize_time(df["AbtTime"])
    df["Source.FileTime"] = df["Source.FileTime"].astype(str).str[:8]  # HH:MM:SS si viene string

    # texto
    varchar_cols = ["Source.Name","DairyName","COW","STAT","BD","TYP",
                    "AbtOffC","AbtCom","AbtCom2",
                    "AbtTech","AbtClvEase","AbtClvEaseCod","AbtDiag",
                    "Source.FileTime","AbtTime"]
    for c in varchar_cols:
        df[c] = df[c].astype(str).where(df[c].notna(), None)

    # COW a texto sin .0
    def cow_to_str(v):
        if pd.isna(v):
            return None
        if isinstance(v, (int, np.integer)):
            return str(int(v))
        if isinstance(v, float) and v.is_integer():
            return str(int(v))
        return str(v)
    df["COW"] = df["COW"].apply(cow_to_str)

    # numéricos
    for c in ["AbtNumLact","AbtCount","AbtCntLf","AbtDim","AbtVAIdCode","AbtCtrlCode","AbtSidEffL2"]:
        df[c] = to_int_nullable(df[c])
    df["AbtCost"] = pd.to_numeric(df["AbtCost"], errors="coerce")
    df["AbtAge"]  = pd.to_numeric(df["AbtAge"],  errors="coerce").round(2)  # DECIMAL(5,2) en SQL

    return df

# ========= UTIL =========
def ts_str(dt: pd.Timestamp) -> str:
    return dt.strftime("%Y-%m-%d %H:%M:%S")

# ========= ÍNDICE SQL + FS =========
def cargar_indice_existente() -> dict:
    try:
        q = """
        SELECT [Source.Name] AS SName,
               CONVERT(varchar(10), [Source.FileDate], 120) + ' ' + CONVERT(varchar(8), [Source.FileTime], 108) AS STS
        FROM dbo.ABORTOS
        WHERE [Source.Name] IS NOT NULL AND [Source.FileDate] IS NOT NULL AND [Source.FileTime] IS NOT NULL
        GROUP BY [Source.Name], [Source.FileDate], [Source.FileTime]
        """
        df = pd.read_sql_query(q, ENGINE)
        return (df.assign(dt=pd.to_datetime(df["STS"], errors="coerce"))
                  .dropna(subset=["dt"])
                  .sort_values(["SName","dt"])
                  .groupby("SName")["STS"].last()
                  .to_dict())
    except Exception as e:
        print(f"No se pudo leer índice existente: {e}")
        return {}

def escanear_archivos_fs() -> set:
    presentes = set()
    for establo in ESTABLOS:
        carpeta = os.path.join(BASE_PATH, establo, "ABORTOS")
        if not os.path.isdir(carpeta):
            continue
        for f in os.listdir(carpeta):
            if f.lower().endswith(".xlsx") and not f.startswith("~$"):
                presentes.add(f)
    return presentes

# ========= BORRADO POR FALTANTES =========
def borrar_faltantes_en_fs():
    indice = cargar_indice_existente()
    en_sql = set(indice.keys())
    en_fs  = escanear_archivos_fs()
    faltantes = sorted(en_sql - en_fs)
    if not faltantes:
        print("✔ No hay archivos eliminados en FS pendientes de borrar en SQL.")
        return
    print(f"🧹 Borrando {len(faltantes)} archivos inexistentes en FS…")
    with ENGINE.begin() as con:
        for nm in faltantes:
            con.execute(text("DELETE FROM dbo.ABORTOS WHERE [Source.Name] = :nm"), {"nm": nm})
            print(f"   - eliminado en SQL: {nm}")

# ========= CARGA INCREMENTAL =========
def cargar_establos_incremental():
    indice = cargar_indice_existente()
    columnas_excel = [c for c in COLUMNAS_SQL if c not in {"Source.Name","Source.FileDate","Source.FileTime"}]

    for establo in ESTABLOS:
        carpeta = os.path.join(BASE_PATH, establo, "ABORTOS")
        if not os.path.isdir(carpeta):
            continue

        archivos = [f for f in os.listdir(carpeta) if f.lower().endswith(".xlsx") and not f.startswith("~$")]

        for archivo in archivos:
            ruta = os.path.join(carpeta, archivo)
            try:
                mtime = pd.to_datetime(os.path.getmtime(ruta), unit="s")
                actual_ts = ts_str(mtime)
                ts_prev = indice.get(archivo)

                if ts_prev is not None and pd.to_datetime(actual_ts) <= pd.to_datetime(ts_prev):
                    print(f"⏩ Saltando {archivo} (sin cambios, mtime {actual_ts})")
                    continue

                if ts_prev is not None and pd.to_datetime(actual_ts) > pd.to_datetime(ts_prev):
                    with ENGINE.begin() as con:
                        con.execute(text("DELETE FROM dbo.ABORTOS WHERE [Source.Name] = :nm"), {"nm": archivo})
                    print(f"🧹 Eliminado previo de {archivo} (TS previo {ts_prev})")

                # leer excel sin encabezado
                df = pd.read_excel(ruta, header=None, names=columnas_excel, engine="openpyxl")
                df.dropna(how="all", inplace=True)
                if df.empty:
                    print(f"⚠ {archivo} vacío, omitido.")
                    continue

                # metadatos
                df["Source.Name"]     = archivo
                df["Source.FileDate"] = mtime.date()
                df["Source.FileTime"] = mtime.strftime("%H:%M:%S")

                # normalizar
                df = normalize_dataframe(df, establo)

                print(f"⏳ Cargando {archivo} desde {establo} ({actual_ts})…")
                df.to_sql("ABORTOS", ENGINE, if_exists="append", index=False, chunksize=1000, schema="dbo")
                print(f"✅ Cargado {archivo}.\n")

                indice[archivo] = actual_ts

            except Exception as e:
                print(f"❌ Error al cargar {archivo} desde {establo}: {e}\n")

if __name__ == "__main__":
    borrar_faltantes_en_fs()
    cargar_establos_incremental()

```

# 3)HATO ACTUAL

```python
import os
import urllib.parse
import pandas as pd
import numpy as np
from sqlalchemy import create_engine, text

# ========= CONFIG =========
SERVER   = "DESKTOP-3I6O45D"
DATABASE = "EVALUACION_ESTABLOS"
USER     = "sa"
PASSWORD = "123456789"

BASE_PATH = r"D:\PRUEBA PYTHON"
ESTABLOS = [
    "AGHOLSTEIN","ANGUS","ANTONIOJAIME","BALI","CANELAS","ESTRELLADEMANUEL",
    "GASUR","HUERTOS SAN MARTIN","LAREDO","MOCHERA","PIAMONTE","REMANSO",
    "SANTA FE","SAUSALITO","SEQUION"
]

# === ORDEN EXACTO DE COLUMNAS A INSERTAR (SIN IDENTITY) ===
COLUMNAS_SQL = [
    "Source.Name","DairyName","TODAY()","cow","gp","lc","dim","stat","statprod","bd","typ",
    "DYP","BIRDAT","AGEM","CLVDAT","SrvDat","SRVSIRE","SrvCount","SReg","SIRE","ASReg1","DAMBN",
    "DamSire:30","DamBd","avmlk","ACMLK","ACMLK_m1","ACMLK_m2","TestMilk","TestMilk_m1",
    "TestMilk_m2","m200","m305","m365","DryDat","RcDry","FECHAPARTOFUTURO",
    "Source.FileDate","Source.FileTime"
]

# ========= CONEXIÓN =========
def _make_engine(driver_name: str, encrypt_yes: bool):
    conn = (
        f"DRIVER={{{driver_name}}};SERVER={SERVER};DATABASE={DATABASE};UID={USER};PWD={PASSWORD};"
        f"TrustServerCertificate=Yes;Encrypt={'Yes' if encrypt_yes else 'No'};"
    )
    return create_engine(
        "mssql+pyodbc:///?odbc_connect=" + urllib.parse.quote_plus(conn),
        fast_executemany=True, pool_pre_ping=True
    )

def get_engine():
    for drv, enc in [("ODBC Driver 18 for SQL Server", True),
                     ("ODBC Driver 17 for SQL Server", False)]:
        try:
            eng = _make_engine(drv, enc)
            with eng.connect():
                pass
            print(f"Conectado con {drv}")
            return eng
        except Exception as e:
            print(f"No se pudo con {drv}: {e}")
    raise RuntimeError("No se pudo abrir conexión ODBC 17/18.")

ENGINE = get_engine()

# ========= NORMALIZACIONES =========
def to_date_series(s: pd.Series) -> pd.Series:
    return pd.to_datetime(s, errors="coerce", dayfirst=True).dt.date

def excel_days_to_time_from_series(s: pd.Series) -> pd.Series:
    td = pd.to_timedelta(pd.to_numeric(s, errors="coerce"), unit="D")
    return (pd.Timestamp("1899-12-30") + td).dt.time

def format_time_hms_series(tser: pd.Series) -> pd.Series:
    def fmt(x):
        if pd.isna(x):
            return None
        try:
            return x.strftime("%H:%M:%S")
        except Exception:
            return None
    return tser.apply(fmt)

def normalize_time(col: pd.Series) -> pd.Series:
    s = col.copy()
    if pd.api.types.is_timedelta64_dtype(s):
        t = (pd.Timestamp("1900-01-01") + s).dt.time
        return format_time_hms_series(t)
    if pd.api.types.is_numeric_dtype(s):
        t = excel_days_to_time_from_series(s)
        return format_time_hms_series(t)
    t = pd.to_datetime(s, errors="coerce").dt.time
    return format_time_hms_series(t)

def to_int_nullable(series: pd.Series) -> pd.Series:
    x = pd.to_numeric(series, errors="coerce")
    x = x.where(x.isna() | (np.mod(x, 1) == 0))
    return x.astype("Int64")

def normalize_dataframe(df: pd.DataFrame, establo: str) -> pd.DataFrame:
    # crear faltantes y ordenar
    for c in COLUMNAS_SQL:
        if c not in df.columns:
            df[c] = np.nan
    df = df[COLUMNAS_SQL]

    # defaults
    df["DairyName"] = df["DairyName"].fillna(establo)

    # fechas
    for c in ["TODAY()","BIRDAT","CLVDAT","SrvDat","DryDat","RcDry","FECHAPARTOFUTURO","Source.FileDate"]:
        df[c] = to_date_series(df[c])

    # hora (guardar como HH:MM:SS; SQL Server lo convierte a time(3))
    df["Source.FileTime"] = normalize_time(df["Source.FileTime"])

    # texto
    varchar_cols = [
        "Source.Name","DairyName","cow","gp","stat","statprod","bd","typ",
        "SRVSIRE","SReg","SIRE","ASReg1","DAMBN","DamSire:30"
    ]
    for c in varchar_cols:
        df[c] = df[c].astype(str).where(df[c].notna(), None)

    # COW: quitar .0 si viene numérica
    def cow_to_str(v):
        if pd.isna(v):
            return None
        if isinstance(v, (int, np.integer)): return str(int(v))
        if isinstance(v, float) and v.is_integer(): return str(int(v))
        return str(v)
    df["cow"] = df["cow"].apply(cow_to_str)

    # enteros
    for c in ["lc","dim","DYP","SrvCount"]:
        df[c] = to_int_nullable(df[c])

    # decimales
    for c in ["AGEM","avmlk","ACMLK","ACMLK_m1","ACMLK_m2","TestMilk","TestMilk_m1","TestMilk_m2","m200","m305","m365"]:
        df[c] = pd.to_numeric(df[c], errors="coerce")

    return df

def ts_str(dt: pd.Timestamp) -> str:
    return dt.strftime("%Y-%m-%d %H:%M:%S")

# ========= ÍNDICE SQL + FS =========
def cargar_indice_existente() -> dict:
    try:
        q = """
        SELECT [Source.Name] AS SName,
               CONVERT(varchar(10), [Source.FileDate], 120) + ' ' + CONVERT(varchar(8), [Source.FileTime], 108) AS STS
        FROM dbo.HATO_ACTUAL
        WHERE [Source.Name] IS NOT NULL AND [Source.FileDate] IS NOT NULL AND [Source.FileTime] IS NOT NULL
        GROUP BY [Source.Name], [Source.FileDate], [Source.FileTime]
        """
        df = pd.read_sql_query(q, ENGINE)
        return (df.assign(dt=pd.to_datetime(df["STS"], errors="coerce"))
                  .dropna(subset=["dt"])
                  .sort_values(["SName","dt"])
                  .groupby("SName")["STS"].last()
                  .to_dict())
    except Exception as e:
        print(f"No se pudo leer índice existente: {e}")
        return {}

def escanear_archivos_fs() -> set:
    presentes = set()
    for establo in ESTABLOS:
        carpeta = os.path.join(BASE_PATH, establo, "HATO ACTUAL")
        if not os.path.isdir(carpeta):
            continue
        for f in os.listdir(carpeta):
            if f.lower().endswith(".xlsx") and not f.startswith("~$"):
                presentes.add(f)
    return presentes

# ========= BORRADO POR FALTANTES =========
def borrar_faltantes_en_fs():
    indice = cargar_indice_existente()
    en_sql = set(indice.keys())
    en_fs  = escanear_archivos_fs()
    faltantes = sorted(en_sql - en_fs)
    if not faltantes:
        print("✔ No hay archivos eliminados en FS pendientes de borrar en dbo.HATO_ACTUAL.")
        return
    print(f"🧹 Borrando {len(faltantes)} archivos inexistentes en FS…")
    with ENGINE.begin() as con:
        for nm in faltantes:
            con.execute(text("DELETE FROM dbo.HATO_ACTUAL WHERE [Source.Name] = :nm"), {"nm": nm})
            print(f"   - eliminado en SQL: {nm}")

# ========= CARGA INCREMENTAL =========
def cargar_establos_incremental():
    indice = cargar_indice_existente()
    # columnas que vienen en el Excel (sin metadatos Source.*)
    columnas_excel = [c for c in COLUMNAS_SQL if c not in {"Source.Name","Source.FileDate","Source.FileTime"}]

    for establo in ESTABLOS:
        carpeta = os.path.join(BASE_PATH, establo, "HATO ACTUAL")
        if not os.path.isdir(carpeta):
            continue

        archivos = [f for f in os.listdir(carpeta) if f.lower().endswith(".xlsx") and not f.startswith("~$")]

        for archivo in archivos:
            ruta = os.path.join(carpeta, archivo)
            try:
                mtime = pd.to_datetime(os.path.getmtime(ruta), unit="s")
                actual_ts = ts_str(mtime)
                ts_prev = indice.get(archivo)

                if ts_prev is not None and pd.to_datetime(actual_ts) <= pd.to_datetime(ts_prev):
                    print(f"⏩ Saltando {archivo} (sin cambios, mtime {actual_ts})")
                    continue

                if ts_prev is not None and pd.to_datetime(actual_ts) > pd.to_datetime(ts_prev):
                    with ENGINE.begin() as con:
                        con.execute(text("DELETE FROM dbo.HATO_ACTUAL WHERE [Source.Name] = :nm"), {"nm": archivo})
                    print(f"🧹 Eliminado previo de {archivo} (TS previo {ts_prev})")

                # leer excel SIN encabezado, en el orden de columnas_excel
                df = pd.read_excel(ruta, header=None, names=columnas_excel, engine="openpyxl")
                df.dropna(how="all", inplace=True)
                if df.empty:
                    print(f"⚠ {archivo} vacío, omitido.")
                    continue

                # metadatos
                df["Source.Name"]     = archivo
                df["Source.FileDate"] = mtime.date()
                df["Source.FileTime"] = mtime.strftime("%H:%M:%S")

                # normalizar tipos
                df = normalize_dataframe(df, establo)

                print(f"⏳ Cargando {archivo} desde {establo} ({actual_ts}) a dbo.HATO_ACTUAL…")
                df.to_sql("HATO_ACTUAL", ENGINE, if_exists="append", index=False, chunksize=1000, schema="dbo")
                print(f"✅ Cargado {archivo}.\n")

                indice[archivo] = actual_ts

            except Exception as e:
                print(f"❌ Error al cargar {archivo} desde {establo}: {e}\n")

if __name__ == "__main__":
    borrar_faltantes_en_fs()
    cargar_establos_incremental()

```

# 4)SECADO

```python
import os
import urllib.parse
import pandas as pd
import numpy as np
from sqlalchemy import create_engine, text

# ========= CONFIG =========
SERVER   = "DESKTOP-3I6O45D"
DATABASE = "EVALUACION_ESTABLOS"
USER     = "sa"
PASSWORD = "123456789"

BASE_PATH = r"D:\PRUEBA PYTHON"
ESTABLOS = [
    "AGHOLSTEIN","ANGUS","ANTONIOJAIME","BALI","CANELAS","ESTRELLADEMANUEL",
    "GASUR","HUERTOS SAN MARTIN","LAREDO","MOCHERA","PIAMONTE","REMANSO",
    "SANTA FE","SAUSALITO","SEQUION"
]

# === ORDEN EXACTO DE COLUMNAS A INSERTAR (SIN IDENTITY) ===
COLUMNAS_SQL = [
    "Source.Name","DairyName","TODAY","cow","stat","bd","typ",
    "DryVAIdCode","DryNumLact","DryCtrlCode","DryOffC","DryCom","DryCom2",
    "DryCount","DryCntLf","DryCost","DryRevCode","DryDim","DryAge","DryTech",
    "DryDat","DryTime","DrySidEffL2","DryDiag","FECHAPARTO",
    "Source.FileDate","Source.FileTime"
]

# ========= CONEXIÓN =========
def _make_engine(driver_name: str, encrypt_yes: bool):
    conn = (
        f"DRIVER={{{driver_name}}};SERVER={SERVER};DATABASE={DATABASE};UID={USER};PWD={PASSWORD};"
        f"TrustServerCertificate=Yes;Encrypt={'Yes' if encrypt_yes else 'No'};"
    )
    return create_engine(
        "mssql+pyodbc:///?odbc_connect=" + urllib.parse.quote_plus(conn),
        fast_executemany=True, pool_pre_ping=True
    )

def get_engine():
    for drv, enc in [("ODBC Driver 18 for SQL Server", True),
                     ("ODBC Driver 17 for SQL Server", False)]:
        try:
            eng = _make_engine(drv, enc)
            with eng.connect():
                pass
            print(f"Conectado con {drv}")
            return eng
        except Exception as e:
            print(f"No se pudo con {drv}: {e}")
    raise RuntimeError("No se pudo abrir conexión ODBC 17/18.")

ENGINE = get_engine()

# ========= NORMALIZACIONES =========
def to_date_series(s: pd.Series) -> pd.Series:
    return pd.to_datetime(s, errors="coerce", dayfirst=True).dt.date

def excel_days_to_time_from_series(s: pd.Series) -> pd.Series:
    td = pd.to_timedelta(pd.to_numeric(s, errors="coerce"), unit="D")
    return (pd.Timestamp("1899-12-30") + td).dt.time

def format_time_hms_series(tser: pd.Series) -> pd.Series:
    def fmt(x):
        if pd.isna(x):
            return None
        try:
            return x.strftime("%H:%M:%S")
        except Exception:
            return None
    return tser.apply(fmt)

def normalize_time(col: pd.Series) -> pd.Series:
    s = col.copy()
    if pd.api.types.is_timedelta64_dtype(s):
        t = (pd.Timestamp("1900-01-01") + s).dt.time
        return format_time_hms_series(t)
    if pd.api.types.is_numeric_dtype(s):
        t = excel_days_to_time_from_series(s)
        return format_time_hms_series(t)
    t = pd.to_datetime(s, errors="coerce").dt.time
    return format_time_hms_series(t)

def to_int_nullable(series: pd.Series) -> pd.Series:
    x = pd.to_numeric(series, errors="coerce")
    x = x.where(x.isna() | (np.mod(x, 1) == 0))
    return x.astype("Int64")

def normalize_dataframe(df: pd.DataFrame, establo: str) -> pd.DataFrame:
    # Crear faltantes y ordenar
    for c in COLUMNAS_SQL:
        if c not in df.columns:
            df[c] = np.nan
    df = df[COLUMNAS_SQL]

    # Defaults
    df["DairyName"] = df["DairyName"].fillna(establo)

    # Fechas
    for c in ["TODAY","DryDat","FECHAPARTO"]:
        df[c] = to_date_series(df[c])

    # Horas
    df["DryTime"]        = normalize_time(df["DryTime"])
    df["Source.FileTime"] = normalize_time(df["Source.FileTime"])

    # Texto
    varchar_cols = [
        "Source.Name","DairyName","cow","stat","bd","typ",
        "DryOffC","DryCom","DryCom2","DryTech","DrySidEffL2","DryDiag"
    ]
    for c in varchar_cols:
        df[c] = df[c].astype(str).where(df[c].notna(), None)

    # COW a string sin “.0”
    def cow_to_str(v):
        if pd.isna(v): return None
        if isinstance(v, (int, np.integer)): return str(int(v))
        if isinstance(v, float) and v.is_integer(): return str(int(v))
        return str(v)
    df["cow"] = df["cow"].apply(cow_to_str)

    # Enteros
    for c in ["DryVAIdCode","DryNumLact","DryCtrlCode","DryCount","DryCntLf","DryRevCode","DryDim"]:
        df[c] = to_int_nullable(df[c])

    # Decimales
    df["DryCost"] = pd.to_numeric(df["DryCost"], errors="coerce")
    df["DryAge"]  = pd.to_numeric(df["DryAge"],  errors="coerce").round(2)

    return df

def ts_str(dt: pd.Timestamp) -> str:
    return dt.strftime("%Y-%m-%d %H:%M:%S")

# ========= ÍNDICE SQL + FS =========
def cargar_indice_existente() -> dict:
    try:
        q = """
        SELECT [Source.Name] AS SName,
               CONVERT(varchar(10), [Source.FileDate], 120) + ' ' + CONVERT(varchar(8), [Source.FileTime], 108) AS STS
        FROM dbo.SECADO
        WHERE [Source.Name] IS NOT NULL AND [Source.FileDate] IS NOT NULL AND [Source.FileTime] IS NOT NULL
        GROUP BY [Source.Name], [Source.FileDate], [Source.FileTime]
        """
        df = pd.read_sql_query(q, ENGINE)
        return (df.assign(dt=pd.to_datetime(df["STS"], errors="coerce"))
                  .dropna(subset=["dt"])
                  .sort_values(["SName","dt"])
                  .groupby("SName")["STS"].last()
                  .to_dict())
    except Exception as e:
        print(f"No se pudo leer índice existente: {e}")
        return {}

def escanear_archivos_fs() -> set:
    presentes = set()
    for establo in ESTABLOS:
        carpeta = os.path.join(BASE_PATH, establo, "SECADO")
        if not os.path.isdir(carpeta):
            continue
        for f in os.listdir(carpeta):
            if f.lower().endswith(".xlsx") and not f.startswith("~$"):
                presentes.add(f)
    return presentes

# ========= BORRADO POR FALTANTES =========
def borrar_faltantes_en_fs():
    indice = cargar_indice_existente()
    en_sql = set(indice.keys())
    en_fs  = escanear_archivos_fs()
    faltantes = sorted(en_sql - en_fs)
    if not faltantes:
        print("✔ No hay archivos eliminados en FS pendientes de borrar en dbo.SECADO.")
        return
    print(f"🧹 Borrando {len(faltantes)} archivos inexistentes en FS…")
    with ENGINE.begin() as con:
        for nm in faltantes:
            con.execute(text("DELETE FROM dbo.SECADO WHERE [Source.Name] = :nm"), {"nm": nm})
            print(f"   - eliminado en SQL: {nm}")

# ========= CARGA INCREMENTAL =========
def cargar_establos_incremental():
    indice = cargar_indice_existente()
    columnas_excel = [c for c in COLUMNAS_SQL if c not in {"Source.Name","Source.FileDate","Source.FileTime"}]

    for establo in ESTABLOS:
        carpeta = os.path.join(BASE_PATH, establo, "SECADO")
        if not os.path.isdir(carpeta):
            continue

        archivos = [f for f in os.listdir(carpeta) if f.lower().endswith(".xlsx") and not f.startswith("~$")]

        for archivo in archivos:
            ruta = os.path.join(carpeta, archivo)
            try:
                mtime = pd.to_datetime(os.path.getmtime(ruta), unit="s")
                actual_ts = ts_str(mtime)
                ts_prev = indice.get(archivo)

                if ts_prev is not None and pd.to_datetime(actual_ts) <= pd.to_datetime(ts_prev):
                    print(f"⏩ Saltando {archivo} (sin cambios, mtime {actual_ts})")
                    continue

                if ts_prev is not None and pd.to_datetime(actual_ts) > pd.to_datetime(ts_prev):
                    with ENGINE.begin() as con:
                        con.execute(text("DELETE FROM dbo.SECADO WHERE [Source.Name] = :nm"), {"nm": archivo})
                    print(f"🧹 Eliminado previo de {archivo} (TS previo {ts_prev})")

                # Leer excel SIN encabezado, con nombres en el orden esperado
                df = pd.read_excel(ruta, header=None, names=columnas_excel, engine="openpyxl")
                df.dropna(how="all", inplace=True)
                if df.empty:
                    print(f"⚠ {archivo} vacío, omitido.")
                    continue

                # Metadatos
                df["Source.Name"]     = archivo
                df["Source.FileDate"] = mtime.date()
                df["Source.FileTime"] = mtime.strftime("%H:%M:%S")

                # Normalizar tipos
                df = normalize_dataframe(df, establo)

                print(f"⏳ Cargando {archivo} desde {establo} ({actual_ts}) a dbo.SECADO…")
                df.to_sql("SECADO", ENGINE, if_exists="append", index=False, chunksize=1000, schema="dbo")
                print(f"✅ Cargado {archivo}.\n")

                indice[archivo] = actual_ts

            except Exception as e:
                print(f"❌ Error al cargar {archivo} desde {establo}: {e}\n")

if __name__ == "__main__":
    borrar_faltantes_en_fs()
    cargar_establos_incremental()

```

# 5)SACA

```python
import os
import urllib.parse
import pandas as pd
import numpy as np
from sqlalchemy import create_engine, text

# ========= CONFIG =========
SERVER   = "DESKTOP-3I6O45D"
DATABASE = "EVALUACION_ESTABLOS"
USER     = "sa"
PASSWORD = "123456789"

BASE_PATH = r"D:\PRUEBA PYTHON"
ESTABLOS = [
    "AGHOLSTEIN","ANGUS","ANTONIOJAIME","BALI","CANELAS","ESTRELLADEMANUEL",
    "GASUR","HUERTOS SAN MARTIN","LAREDO","MOCHERA","PIAMONTE","REMANSO",
    "SANTA FE","SAUSALITO","SEQUION"
]

# === ORDEN EXACTO DE COLUMNAS A INSERTAR (SIN IDENTITY) ===
COLUMNAS_SQL = [
    "Source.Name","DairyName","TODAY","cow","LC","DIM","stat","BD","TYP",
    "BelHeight","LHReas","LHDat","LHType","IMBelHeight","BUYER",
    "FECHA_DE_PARTO","BIRDAT","VETCOM","VETCOM2","GP",
    "Source.FileDate","Source.FileTime"
]

# ========= CONEXIÓN =========
def _make_engine(driver_name: str, encrypt_yes: bool):
    conn = (
        f"DRIVER={{{driver_name}}};SERVER={SERVER};DATABASE={DATABASE};UID={USER};PWD={PASSWORD};"
        f"TrustServerCertificate=Yes;Encrypt={'Yes' if encrypt_yes else 'No'};"
    )
    return create_engine(
        "mssql+pyodbc:///?odbc_connect=" + urllib.parse.quote_plus(conn),
        fast_executemany=True, pool_pre_ping=True
    )

def get_engine():
    for drv, enc in [("ODBC Driver 18 for SQL Server", True),
                     ("ODBC Driver 17 for SQL Server", False)]:
        try:
            eng = _make_engine(drv, enc)
            with eng.connect():
                pass
            print(f"Conectado con {drv}")
            return eng
        except Exception as e:
            print(f"No se pudo con {drv}: {e}")
    raise RuntimeError("No se pudo abrir conexión ODBC 17/18.")

ENGINE = get_engine()

# ========= NORMALIZACIONES =========
def to_date_series(s: pd.Series) -> pd.Series:
    return pd.to_datetime(s, errors="coerce", dayfirst=True).dt.date

def excel_days_to_time_from_series(s: pd.Series) -> pd.Series:
    td = pd.to_timedelta(pd.to_numeric(s, errors="coerce"), unit="D")
    return (pd.Timestamp("1899-12-30") + td).dt.time

def format_time_hms_series(tser: pd.Series) -> pd.Series:
    def fmt(x):
        if pd.isna(x):
            return None
        try:
            return x.strftime("%H:%M:%S")
        except Exception:
            return None
    return tser.apply(fmt)

def normalize_time(col: pd.Series) -> pd.Series:
    s = col.copy()
    if pd.api.types.is_timedelta64_dtype(s):
        t = (pd.Timestamp("1900-01-01") + s).dt.time
        return format_time_hms_series(t)
    if pd.api.types.is_numeric_dtype(s):
        t = excel_days_to_time_from_series(s)
        return format_time_hms_series(t)
    t = pd.to_datetime(s, errors="coerce").dt.time
    return format_time_hms_series(t)

def to_int_nullable(series: pd.Series) -> pd.Series:
    x = pd.to_numeric(series, errors="coerce")
    x = x.where(x.isna() | (np.mod(x, 1) == 0))
    return x.astype("Int64")

def to_decimal_nullable(series: pd.Series) -> pd.Series:
    return pd.to_numeric(series, errors="coerce")

def cow_to_str(v):
    if pd.isna(v): return None
    if isinstance(v, (int, np.integer)): return str(int(v))
    if isinstance(v, float) and v.is_integer(): return str(int(v))
    return str(v)

def normalize_dataframe(df: pd.DataFrame, establo: str) -> pd.DataFrame:
    # Crear faltantes y ordenar
    for c in COLUMNAS_SQL:
        if c not in df.columns:
            df[c] = np.nan
    df = df[COLUMNAS_SQL]

    # Defaults
    df["DairyName"] = df["DairyName"].fillna(establo)

    # Fechas
    for c in ["TODAY","LHDat","FECHA_DE_PARTO","BIRDAT","Source.FileDate"]:
        df[c] = to_date_series(df[c])

    # Hora
    df["Source.FileTime"] = normalize_time(df["Source.FileTime"])

    # Texto
    for c in ["Source.Name","DairyName","cow","stat","BD","TYP","LHReas","LHType",
              "BUYER","VETCOM","VETCOM2","GP"]:
        df[c] = df[c].astype(str).where(df[c].notna(), None)

    # COW a string sin .0
    df["cow"] = df["cow"].apply(cow_to_str)

    # Enteros
    for c in ["LC","DIM"]:
        df[c] = to_int_nullable(df[c])

    # Decimales
    for c in ["BelHeight","IMBelHeight"]:
        df[c] = to_decimal_nullable(df[c])

    # Timedelta a texto si apareciera en algún campo
    for c in df.columns:
        if pd.api.types.is_timedelta64_dtype(df[c]):
            df[c] = format_time_hms_series((pd.Timestamp("1900-01-01") + df[c]).dt.time)

    return df

def ts_str(dt: pd.Timestamp) -> str:
    return dt.strftime("%Y-%m-%d %H:%M:%S")

# ========= ÍNDICE SQL + FS =========
def cargar_indice_existente() -> dict:
    try:
        q = """
        SELECT [Source.Name] AS SName,
               CONVERT(varchar(10), [Source.FileDate], 120) + ' ' + CONVERT(varchar(8), [Source.FileTime], 108) AS STS
        FROM dbo.SACAS
        WHERE [Source.Name] IS NOT NULL AND [Source.FileDate] IS NOT NULL AND [Source.FileTime] IS NOT NULL
        GROUP BY [Source.Name], [Source.FileDate], [Source.FileTime]
        """
        df = pd.read_sql_query(q, ENGINE)
        return (df.assign(dt=pd.to_datetime(df["STS"], errors="coerce"))
                  .dropna(subset=["dt"])
                  .sort_values(["SName","dt"])
                  .groupby("SName")["STS"].last()
                  .to_dict())
    except Exception as e:
        print(f"No se pudo leer índice existente: {e}")
        return {}

def escanear_archivos_fs() -> set:
    presentes = set()
    for establo in ESTABLOS:
        carpeta = os.path.join(BASE_PATH, establo, "SACAS")
        if not os.path.isdir(carpeta):
            continue
        for f in os.listdir(carpeta):
            if f.lower().endswith(".xlsx") and not f.startswith("~$"):
                presentes.add(f)
    return presentes

# ========= BORRADO POR FALTANTES =========
def borrar_faltantes_en_fs():
    indice = cargar_indice_existente()
    en_sql = set(indice.keys())
    en_fs  = escanear_archivos_fs()
    faltantes = sorted(en_sql - en_fs)
    if not faltantes:
        print("✔ No hay archivos eliminados en FS pendientes de borrar en dbo.SACAS.")
        return
    print(f"🧹 Borrando {len(faltantes)} archivos inexistentes en FS…")
    with ENGINE.begin() as con:
        for nm in faltantes:
            con.execute(text("DELETE FROM dbo.SACAS WHERE [Source.Name] = :nm"), {"nm": nm})
            print(f"   - eliminado en SQL: {nm}")

# ========= CARGA INCREMENTAL =========
def cargar_establos_incremental():
    indice = cargar_indice_existente()
    columnas_excel = [c for c in COLUMNAS_SQL if c not in {"Source.Name","Source.FileDate","Source.FileTime"}]

    for establo in ESTABLOS:
        carpeta = os.path.join(BASE_PATH, establo, "SACAS")
        if not os.path.isdir(carpeta):
            continue

        archivos = [f for f in os.listdir(carpeta)
                    if f.lower().endswith(".xlsx") and not f.startswith("~$")]

        for archivo in archivos:
            ruta = os.path.join(carpeta, archivo)
            try:
                mtime = pd.to_datetime(os.path.getmtime(ruta), unit="s")
                actual_ts = ts_str(mtime)
                ts_prev = indice.get(archivo)

                if ts_prev is not None and pd.to_datetime(actual_ts) <= pd.to_datetime(ts_prev):
                    print(f"⏩ Saltando {archivo} (sin cambios, mtime {actual_ts})")
                    continue

                if ts_prev is not None and pd.to_datetime(actual_ts) > pd.to_datetime(ts_prev):
                    with ENGINE.begin() as con:
                        con.execute(text("DELETE FROM dbo.SACAS WHERE [Source.Name] = :nm"), {"nm": archivo})
                    print(f"🧹 Eliminado previo de {archivo} (TS previo {ts_prev})")

                # Leer excel SIN encabezado, con nombres en el orden esperado
                df = pd.read_excel(ruta, header=None, names=columnas_excel, engine="openpyxl")
                df.dropna(how="all", inplace=True)
                if df.empty:
                    print(f"⚠ {archivo} vacío, omitido.")
                    continue

                # Metadatos
                df["Source.Name"]     = archivo
                df["Source.FileDate"] = mtime.date()
                df["Source.FileTime"] = mtime.strftime("%H:%M:%S")

                # Normalizar tipos
                df = normalize_dataframe(df, establo)

                print(f"⏳ Cargando {archivo} desde {establo} ({actual_ts}) a dbo.SACAS…")
                df.to_sql("SACAS", ENGINE, if_exists="append", index=False, chunksize=1000, schema="dbo")
                print(f"✅ Cargado {archivo}.\n")

                indice[archivo] = actual_ts

            except Exception as e:
                print(f"❌ Error al cargar {archivo} desde {establo}: {e}\n")

if __name__ == "__main__":
    borrar_faltantes_en_fs()
    cargar_establos_incremental()

```

# 6)PROGENIE

```python
import os
import urllib.parse
import pandas as pd
import numpy as np
from sqlalchemy import create_engine, text

# ========= CONFIG =========
SERVER   = "DESKTOP-3I6O45D"
DATABASE = "EVALUACION_ESTABLOS"
USER     = "sa"
PASSWORD = "123456789"

BASE_PATH = r"D:\PRUEBA PYTHON"
ESTABLOS = [
    "AGHOLSTEIN","ANGUS","ANTONIOJAIME","BALI","CANELAS","ESTRELLADEMANUEL",
    "GASUR","HUERTOS SAN MARTIN","LAREDO","MOCHERA","PIAMONTE","REMANSO",
    "SANTA FE","SAUSALITO","SEQUION"
]

# === ORDEN EXACTO DE COLUMNAS A INSERTAR (SIN IDENTITY) ===
COLUMNAS_SQL = [
    "Source.Name","DairyName","TODAY","COW","STAT","BD","TYP",
    "OffsBirth","OffsETDam","OffsETFlag","OffsSire","Offspring","OffsSex","OffsValue",
    "Source.FileDate","Source.FileTime"
]

# ========= CONEXIÓN =========
def _make_engine(driver_name: str, encrypt_yes: bool):
    conn = (
        f"DRIVER={{{driver_name}}};SERVER={SERVER};DATABASE={DATABASE};UID={USER};PWD={PASSWORD};"
        f"TrustServerCertificate=Yes;Encrypt={'Yes' if encrypt_yes else 'No'};"
    )
    return create_engine(
        "mssql+pyodbc:///?odbc_connect=" + urllib.parse.quote_plus(conn),
        fast_executemany=True, pool_pre_ping=True
    )

def get_engine():
    for drv, enc in [("ODBC Driver 18 for SQL Server", True),
                     ("ODBC Driver 17 for SQL Server", False)]:
        try:
            eng = _make_engine(drv, enc)
            with eng.connect():
                pass
            print(f"Conectado con {drv}")
            return eng
        except Exception as e:
            print(f"No se pudo con {drv}: {e}")
    raise RuntimeError("No se pudo abrir conexión ODBC 17/18.")

ENGINE = get_engine()

# ========= NORMALIZACIONES =========
def to_date_series(s: pd.Series) -> pd.Series:
    return pd.to_datetime(s, errors="coerce", dayfirst=True).dt.date

def excel_days_to_time_from_series(s: pd.Series) -> pd.Series:
    td = pd.to_timedelta(pd.to_numeric(s, errors="coerce"), unit="D")
    return (pd.Timestamp("1899-12-30") + td).dt.time

def format_time_hms_series(tser: pd.Series) -> pd.Series:
    def fmt(x):
        if pd.isna(x):
            return None
        try:
            return x.strftime("%H:%M:%S")
        except Exception:
            return None
    return tser.apply(fmt)

def normalize_time(col: pd.Series) -> pd.Series:
    s = col.copy()
    if pd.api.types.is_timedelta64_dtype(s):
        t = (pd.Timestamp("1900-01-01") + s).dt.time
        return format_time_hms_series(t)
    if pd.api.types.is_numeric_dtype(s):
        t = excel_days_to_time_from_series(s)
        return format_time_hms_series(t)
    t = pd.to_datetime(s, errors="coerce").dt.time
    return format_time_hms_series(t)

def to_decimal_nullable(series: pd.Series) -> pd.Series:
    return pd.to_numeric(series, errors="coerce")

def cow_to_str(v):
    if pd.isna(v): return None
    if isinstance(v, (int, np.integer)): return str(int(v))
    if isinstance(v, float) and v.is_integer(): return str(int(v))
    return str(v)

def normalize_dataframe(df: pd.DataFrame, establo: str) -> pd.DataFrame:
    # Asegurar columnas y orden
    for c in COLUMNAS_SQL:
        if c not in df.columns:
            df[c] = np.nan
    df = df[COLUMNAS_SQL]

    # Defaults
    df["DairyName"] = df["DairyName"].fillna(establo)

    # Fechas
    for c in ["TODAY","OffsBirth","Source.FileDate"]:
        df[c] = to_date_series(df[c])

    # Hora
    df["Source.FileTime"] = normalize_time(df["Source.FileTime"])

    # Texto
    for c in ["Source.Name","DairyName","COW","BD","STAT","TYP",
              "OffsETDam","OffsETFlag","OffsSire","Offspring","OffsSex"]:
        df[c] = df[c].astype(str).where(df[c].notna(), None)

    # COW a string sin .0
    df["COW"] = df["COW"].apply(cow_to_str)

    # Decimal
    df["OffsValue"] = to_decimal_nullable(df["OffsValue"])

    # Timedelta defensivo
    for c in df.columns:
        if pd.api.types.is_timedelta64_dtype(df[c]):
            df[c] = format_time_hms_series((pd.Timestamp("1900-01-01") + df[c]).dt.time)

    return df

def ts_str(dt: pd.Timestamp) -> str:
    return dt.strftime("%Y-%m-%d %H:%M:%S")

# ========= ÍNDICE SQL + FS =========
def cargar_indice_existente() -> dict:
    try:
        q = """
        SELECT [Source.Name] AS SName,
               CONVERT(varchar(10), [Source.FileDate], 120) + ' ' + CONVERT(varchar(8), [Source.FileTime], 108) AS STS
        FROM dbo.PROGENIE
        WHERE [Source.Name] IS NOT NULL AND [Source.FileDate] IS NOT NULL AND [Source.FileTime] IS NOT NULL
        GROUP BY [Source.Name], [Source.FileDate], [Source.FileTime]
        """
        df = pd.read_sql_query(q, ENGINE)
        return (df.assign(dt=pd.to_datetime(df["STS"], errors="coerce"))
                  .dropna(subset=["dt"])
                  .sort_values(["SName","dt"])
                  .groupby("SName")["STS"].last()
                  .to_dict())
    except Exception as e:
        print(f"No se pudo leer índice existente: {e}")
        return {}

def escanear_archivos_fs() -> set:
    presentes = set()
    for establo in ESTABLOS:
        carpeta = os.path.join(BASE_PATH, establo, "PROGENIE")
        if not os.path.isdir(carpeta):
            continue
        for f in os.listdir(carpeta):
            if f.lower().endswith(".xlsx") and not f.startswith("~$"):
                presentes.add(f)
    return presentes

# ========= BORRADO POR FALTANTES =========
def borrar_faltantes_en_fs():
    indice = cargar_indice_existente()
    en_sql = set(indice.keys())
    en_fs  = escanear_archivos_fs()
    faltantes = sorted(en_sql - en_fs)
    if not faltantes:
        print("✔ No hay archivos eliminados en FS pendientes de borrar en dbo.PROGENIE.")
        return
    print(f"🧹 Borrando {len(faltantes)} archivos inexistentes en FS…")
    with ENGINE.begin() as con:
        for nm in faltantes:
            con.execute(text("DELETE FROM dbo.PROGENIE WHERE [Source.Name] = :nm"), {"nm": nm})
            print(f"   - eliminado en SQL: {nm}")

# ========= CARGA INCREMENTAL =========
def cargar_establos_incremental():
    indice = cargar_indice_existente()
    columnas_excel = [c for c in COLUMNAS_SQL if c not in {"Source.Name","Source.FileDate","Source.FileTime"}]

    for establo in ESTABLOS:
        carpeta = os.path.join(BASE_PATH, establo, "PROGENIE")
        if not os.path.isdir(carpeta):
            continue

        archivos = [f for f in os.listdir(carpeta)
                    if f.lower().endswith(".xlsx") and not f.startswith("~$")]

        for archivo in archivos:
            ruta = os.path.join(carpeta, archivo)
            try:
                mtime = pd.to_datetime(os.path.getmtime(ruta), unit="s")
                actual_ts = ts_str(mtime)
                ts_prev = indice.get(archivo)

                if ts_prev is not None and pd.to_datetime(actual_ts) <= pd.to_datetime(ts_prev):
                    print(f"⏩ Saltando {archivo} (sin cambios, mtime {actual_ts})")
                    continue

                if ts_prev is not None and pd.to_datetime(actual_ts) > pd.to_datetime(ts_prev):
                    with ENGINE.begin() as con:
                        con.execute(text("DELETE FROM dbo.PROGENIE WHERE [Source.Name] = :nm"), {"nm": archivo})
                    print(f"🧹 Eliminado previo de {archivo} (TS previo {ts_prev})")

                # Leer excel SIN encabezado, con nombres exactos esperados por la tabla
                df = pd.read_excel(ruta, header=None, names=columnas_excel, engine="openpyxl")
                df.dropna(how="all", inplace=True)
                if df.empty:
                    print(f"⚠ {archivo} vacío, omitido.")
                    continue

                # Metadatos
                df["Source.Name"]     = archivo
                df["Source.FileDate"] = mtime.date()
                df["Source.FileTime"] = mtime.strftime("%H:%M:%S")

                # Normalizar tipos
                df = normalize_dataframe(df, establo)

                print(f"⏳ Cargando {archivo} desde {establo} ({actual_ts}) a dbo.PROGENIE…")
                df.to_sql("PROGENIE", ENGINE, if_exists="append", index=False, chunksize=1000, schema="dbo")
                print(f"✅ Cargado {archivo}.\n")

                indice[archivo] = actual_ts

            except Exception as e:
                print(f"❌ Error al cargar {archivo} desde {establo}: {e}\n")

if __name__ == "__main__":
    borrar_faltantes_en_fs()
    cargar_establos_incremental()

```

# 7)NACIMIENTO

```python
import os
import urllib.parse
import pandas as pd
import numpy as np
from sqlalchemy import create_engine, text

# ========= CONFIG =========
SERVER   = "DESKTOP-3I6O45D"
DATABASE = "EVALUACION_ESTABLOS"
USER     = "sa"
PASSWORD = "123456789"

BASE_PATH = r"D:\PRUEBA PYTHON"
ESTABLOS = [
    "AGHOLSTEIN","ANGUS","ANTONIOJAIME","BALI","CANELAS","ESTRELLADEMANUEL",
    "GASUR","HUERTOS SAN MARTIN","LAREDO","MOCHERA","PIAMONTE","REMANSO",
    "SANTA FE","SAUSALITO","SEQUION"
]

# === ORDEN EXACTO DE COLUMNAS A INSERTAR (SIN IDENTITY) ===
COLUMNAS_SQL = [
    "Source.Name","DairyName","TODAY","cow","stat","bd","typ",
    "BirVAIdCode","BirNumLact","BirCtrlCode","BirOffC","BirCom","BirCom2",
    "BirCount","BirCntLf","BirCost","BirRevCode","BirDim","BirAge","BirTech",
    "BirClvEase","BirClvEaseCod","BirDat","BirTime","BirSidEffL2","BirDiag",
    "Source.FileDate","Source.FileTime"
]
# Columnas que vienen desde Excel (sin metadatos Source.*)
COLUMNAS_EXCEL = [c for c in COLUMNAS_SQL if c not in {"Source.Name","Source.FileDate","Source.FileTime"}]

# ========= CONEXIÓN =========
def _make_engine(driver_name: str, encrypt_yes: bool):
    conn = (
        f"DRIVER={{{driver_name}}};SERVER={SERVER};DATABASE={DATABASE};UID={USER};PWD={PASSWORD};"
        f"TrustServerCertificate=Yes;Encrypt={'Yes' if encrypt_yes else 'No'};"
    )
    return create_engine(
        "mssql+pyodbc:///?odbc_connect=" + urllib.parse.quote_plus(conn),
        fast_executemany=True, pool_pre_ping=True
    )

def get_engine():
    for drv, enc in [("ODBC Driver 18 for SQL Server", True),
                     ("ODBC Driver 17 for SQL Server", False)]:
        try:
            eng = _make_engine(drv, enc)
            with eng.connect():
                pass
            print(f"Conectado con {drv}")
            return eng
        except Exception as e:
            print(f"No se pudo con {drv}: {e}")
    raise RuntimeError("No se pudo abrir conexión ODBC 17/18.")

ENGINE = get_engine()

# ========= NORMALIZACIONES =========
def to_date_series(s: pd.Series) -> pd.Series:
    return pd.to_datetime(s, errors="coerce", dayfirst=True).dt.date

def excel_days_to_time_from_series(s: pd.Series) -> pd.Series:
    td = pd.to_timedelta(pd.to_numeric(s, errors="coerce"), unit="D")
    return (pd.Timestamp("1899-12-30") + td).dt.time

def format_time_hms_series(tser: pd.Series) -> pd.Series:
    def fmt(x):
        if pd.isna(x):
            return None
        try:
            return x.strftime("%H:%M:%S")
        except Exception:
            return None
    return tser.apply(fmt)

def normalize_time(col: pd.Series) -> pd.Series:
    s = col.copy()
    if pd.api.types.is_timedelta64_dtype(s):
        t = (pd.Timestamp("1900-01-01") + s).dt.time
        return format_time_hms_series(t)
    if pd.api.types.is_numeric_dtype(s):
        t = excel_days_to_time_from_series(s)
        return format_time_hms_series(t)
    t = pd.to_datetime(s, errors="coerce").dt.time
    return format_time_hms_series(t)

def to_decimal_nullable(series: pd.Series) -> pd.Series:
    return pd.to_numeric(series, errors="coerce")

def cow_to_str(v):
    if pd.isna(v): return None
    if isinstance(v, (int, np.integer)): return str(int(v))
    if isinstance(v, float) and v.is_integer(): return str(int(v))
    return str(v)

def normalize_dataframe(df: pd.DataFrame, establo: str) -> pd.DataFrame:
    # Asegurar columnas y orden
    for c in COLUMNAS_SQL:
        if c not in df.columns:
            df[c] = np.nan
    df = df[COLUMNAS_SQL]

    # Defaults
    df["DairyName"] = df["DairyName"].fillna(establo)

    # Fechas
    for c in ["TODAY","BirDat","Source.FileDate"]:
        df[c] = to_date_series(df[c])

    # Horas
    for c in ["BirTime","Source.FileTime"]:
        df[c] = normalize_time(df[c])

    # Texto
    for c in ["Source.Name","DairyName","cow","stat","bd","typ","BirOffC","BirCom",
              "BirCom2","BirTech","BirClvEase","BirClvEaseCod","BirSidEffL2","BirDiag"]:
        df[c] = df[c].astype(str).where(df[c].notna(), None)

    # cow a string sin .0
    df["cow"] = df["cow"].apply(cow_to_str)

    # Numéricos
    for c in ["BirNumLact","BirCtrlCode","BirCount","BirCntLf","BirRevCode","BirDim"]:
        df[c] = pd.to_numeric(df[c], errors="coerce").astype("Int64")

    for c in ["BirCost","BirAge"]:
        df[c] = to_decimal_nullable(df[c])

    # Timedelta defensivo
    for c in df.columns:
        if pd.api.types.is_timedelta64_dtype(df[c]):
            df[c] = format_time_hms_series((pd.Timestamp("1900-01-01") + df[c]).dt.time)

    return df

def ts_str(dt: pd.Timestamp) -> str:
    return dt.strftime("%Y-%m-%d %H:%M:%S")

# ========= ÍNDICE SQL + FS =========
def cargar_indice_existente() -> dict:
    try:
        q = """
        SELECT [Source.Name] AS SName,
               CONVERT(varchar(10), [Source.FileDate], 120) + ' ' + CONVERT(varchar(8), [Source.FileTime], 108) AS STS
        FROM dbo.NACIMIENTOS
        WHERE [Source.Name] IS NOT NULL AND [Source.FileDate] IS NOT NULL AND [Source.FileTime] IS NOT NULL
        GROUP BY [Source.Name], [Source.FileDate], [Source.FileTime]
        """
        df = pd.read_sql_query(q, ENGINE)
        return (df.assign(dt=pd.to_datetime(df["STS"], errors="coerce"))
                  .dropna(subset=["dt"])
                  .sort_values(["SName","dt"])
                  .groupby("SName")["STS"].last()
                  .to_dict())
    except Exception as e:
        print(f"No se pudo leer índice existente: {e}")
        return {}

def escanear_archivos_fs() -> set:
    presentes = set()
    for establo in ESTABLOS:
        carpeta = os.path.join(BASE_PATH, establo, "NACIMIENTOS")
        if not os.path.isdir(carpeta):
            continue
        for f in os.listdir(carpeta):
            if f.lower().endswith(".xlsx") and not f.startswith("~$"):
                presentes.add(f)
    return presentes

# ========= BORRADO POR FALTANTES =========
def borrar_faltantes_en_fs():
    indice = cargar_indice_existente()
    en_sql = set(indice.keys())
    en_fs  = escanear_archivos_fs()
    faltantes = sorted(en_sql - en_fs)
    if not faltantes:
        print("✔ No hay archivos eliminados en FS pendientes de borrar en dbo.NACIMIENTOS.")
        return
    print(f"🧹 Borrando {len(faltantes)} archivos inexistentes en FS…")
    with ENGINE.begin() as con:
        for nm in faltantes:
            con.execute(text("DELETE FROM dbo.NACIMIENTOS WHERE [Source.Name] = :nm"), {"nm": nm})
            print(f"   - eliminado en SQL: {nm}")

# ========= CARGA INCREMENTAL =========
def cargar_establos_incremental():
    indice = cargar_indice_existente()

    for establo in ESTABLOS:
        carpeta = os.path.join(BASE_PATH, establo, "NACIMIENTOS")
        if not os.path.isdir(carpeta):
            continue

        archivos = [f for f in os.listdir(carpeta)
                    if f.lower().endswith(".xlsx") and not f.startswith("~$")]

        for archivo in archivos:
            ruta = os.path.join(carpeta, archivo)
            try:
                mtime = pd.to_datetime(os.path.getmtime(ruta), unit="s")
                actual_ts = ts_str(mtime)
                ts_prev = indice.get(archivo)

                if ts_prev is not None and pd.to_datetime(actual_ts) <= pd.to_datetime(ts_prev):
                    print(f"⏩ Saltando {archivo} (sin cambios, mtime {actual_ts})")
                    continue

                if ts_prev is not None and pd.to_datetime(actual_ts) > pd.to_datetime(ts_prev):
                    with ENGINE.begin() as con:
                        con.execute(text("DELETE FROM dbo.NACIMIENTOS WHERE [Source.Name] = :nm"), {"nm": archivo})
                    print(f"🧹 Eliminado previo de {archivo} (TS previo {ts_prev})")

                # Leer excel SIN encabezado usando los nombres esperados
                df = pd.read_excel(ruta, header=None, names=COLUMNAS_EXCEL, engine="openpyxl")
                df.dropna(how="all", inplace=True)
                if df.empty:
                    print(f"⚠ {archivo} vacío, omitido.")
                    continue

                # Metadatos
                df["Source.Name"]     = archivo
                df["Source.FileDate"] = mtime.date()
                df["Source.FileTime"] = mtime.strftime("%H:%M:%S")

                # Normalizar tipos
                df = normalize_dataframe(df, establo)

                print(f"⏳ Cargando {archivo} desde {establo} ({actual_ts}) a dbo.NACIMIENTOS…")
                df.to_sql("NACIMIENTOS", ENGINE, if_exists="append", index=False, chunksize=1000, schema="dbo")
                print(f"✅ Cargado {archivo}.\n")

                indice[archivo] = actual_ts

            except Exception as e:
                print(f"❌ Error al cargar {archivo} desde {establo}: {e}\n")

if __name__ == "__main__":
    borrar_faltantes_en_fs()
    cargar_establos_incremental()

```

# 8)ENFERMEDADES

```python
import os
import urllib.parse
import pandas as pd
import numpy as np
from sqlalchemy import create_engine, text

# ========= CONFIG =========
SERVER   = "DESKTOP-3I6O45D"
DATABASE = "EVALUACION_ESTABLOS"
USER     = "sa"
PASSWORD = "123456789"

BASE_PATH = r"D:\PRUEBA PYTHON"
ESTABLOS = [
    "AGHOLSTEIN","ANGUS","ANTONIOJAIME","BALI","CANELAS","ESTRELLADEMANUEL",
    "GASUR","HUERTOS SAN MARTIN","LAREDO","MOCHERA","PIAMONTE","REMANSO",
    "SANTA FE","SAUSALITO","SEQUION"
]

# === ORDEN EXACTO DE COLUMNAS A INSERTAR (SIN IDENTITY) ===
COLUMNAS_SQL = [
    "Source.Name","DairyName","TODAY","cow","stat","bd","typ",
    "BirVAIdCode","BirNumLact","BirCtrlCode","BirOffC","BirCom","BirCom2",
    "BirCount","BirCntLf","BirCost","BirRevCode","BirDim","BirAge","BirTech",
    "BirClvEase","BirClvEaseCod","BirDat","BirTime","BirSidEffL2","BirDiag",
    "Source.FileDate","Source.FileTime"
]
# Columnas que vienen desde Excel (sin metadatos Source.*)
COLUMNAS_EXCEL = [c for c in COLUMNAS_SQL if c not in {"Source.Name","Source.FileDate","Source.FileTime"}]

# ========= CONEXIÓN =========
def _make_engine(driver_name: str, encrypt_yes: bool):
    conn = (
        f"DRIVER={{{driver_name}}};SERVER={SERVER};DATABASE={DATABASE};UID={USER};PWD={PASSWORD};"
        f"TrustServerCertificate=Yes;Encrypt={'Yes' if encrypt_yes else 'No'};"
    )
    return create_engine(
        "mssql+pyodbc:///?odbc_connect=" + urllib.parse.quote_plus(conn),
        fast_executemany=True, pool_pre_ping=True
    )

def get_engine():
    for drv, enc in [("ODBC Driver 18 for SQL Server", True),
                     ("ODBC Driver 17 for SQL Server", False)]:
        try:
            eng = _make_engine(drv, enc)
            with eng.connect():
                pass
            print(f"Conectado con {drv}")
            return eng
        except Exception as e:
            print(f"No se pudo con {drv}: {e}")
    raise RuntimeError("No se pudo abrir conexión ODBC 17/18.")

ENGINE = get_engine()

# ========= NORMALIZACIONES =========
def to_date_series(s: pd.Series) -> pd.Series:
    return pd.to_datetime(s, errors="coerce", dayfirst=True).dt.date

def excel_days_to_time_from_series(s: pd.Series) -> pd.Series:
    td = pd.to_timedelta(pd.to_numeric(s, errors="coerce"), unit="D")
    return (pd.Timestamp("1899-12-30") + td).dt.time

def format_time_hms_series(tser: pd.Series) -> pd.Series:
    def fmt(x):
        if pd.isna(x):
            return None
        try:
            return x.strftime("%H:%M:%S")
        except Exception:
            return None
    return tser.apply(fmt)

def normalize_time(col: pd.Series) -> pd.Series:
    s = col.copy()
    if pd.api.types.is_timedelta64_dtype(s):
        t = (pd.Timestamp("1900-01-01") + s).dt.time
        return format_time_hms_series(t)
    if pd.api.types.is_numeric_dtype(s):
        t = excel_days_to_time_from_series(s)
        return format_time_hms_series(t)
    t = pd.to_datetime(s, errors="coerce").dt.time
    return format_time_hms_series(t)

def to_decimal_nullable(series: pd.Series) -> pd.Series:
    return pd.to_numeric(series, errors="coerce")

def cow_to_str(v):
    if pd.isna(v): return None
    if isinstance(v, (int, np.integer)): return str(int(v))
    if isinstance(v, float) and v.is_integer(): return str(int(v))
    return str(v)

def normalize_dataframe(df: pd.DataFrame, establo: str) -> pd.DataFrame:
    # Asegurar columnas y orden
    for c in COLUMNAS_SQL:
        if c not in df.columns:
            df[c] = np.nan
    df = df[COLUMNAS_SQL]

    # Defaults
    df["DairyName"] = df["DairyName"].fillna(establo)

    # Fechas
    for c in ["TODAY","BirDat","Source.FileDate"]:
        df[c] = to_date_series(df[c])

    # Horas
    for c in ["BirTime","Source.FileTime"]:
        df[c] = normalize_time(df[c])

    # Texto
    for c in ["Source.Name","DairyName","cow","stat","bd","typ","BirOffC","BirCom",
              "BirCom2","BirTech","BirClvEase","BirClvEaseCod","BirSidEffL2","BirDiag"]:
        df[c] = df[c].astype(str).where(df[c].notna(), None)

    # cow a string sin .0
    df["cow"] = df["cow"].apply(cow_to_str)

    # Numéricos
    for c in ["BirNumLact","BirCtrlCode","BirCount","BirCntLf","BirRevCode","BirDim"]:
        df[c] = pd.to_numeric(df[c], errors="coerce").astype("Int64")

    for c in ["BirCost","BirAge"]:
        df[c] = to_decimal_nullable(df[c])

    # Timedelta defensivo
    for c in df.columns:
        if pd.api.types.is_timedelta64_dtype(df[c]):
            df[c] = format_time_hms_series((pd.Timestamp("1900-01-01") + df[c]).dt.time)

    return df

def ts_str(dt: pd.Timestamp) -> str:
    return dt.strftime("%Y-%m-%d %H:%M:%S")

# ========= ÍNDICE SQL + FS =========
def cargar_indice_existente() -> dict:
    try:
        q = """
        SELECT [Source.Name] AS SName,
               CONVERT(varchar(10), [Source.FileDate], 120) + ' ' + CONVERT(varchar(8), [Source.FileTime], 108) AS STS
        FROM dbo.NACIMIENTOS
        WHERE [Source.Name] IS NOT NULL AND [Source.FileDate] IS NOT NULL AND [Source.FileTime] IS NOT NULL
        GROUP BY [Source.Name], [Source.FileDate], [Source.FileTime]
        """
        df = pd.read_sql_query(q, ENGINE)
        return (df.assign(dt=pd.to_datetime(df["STS"], errors="coerce"))
                  .dropna(subset=["dt"])
                  .sort_values(["SName","dt"])
                  .groupby("SName")["STS"].last()
                  .to_dict())
    except Exception as e:
        print(f"No se pudo leer índice existente: {e}")
        return {}

def escanear_archivos_fs() -> set:
    presentes = set()
    for establo in ESTABLOS:
        carpeta = os.path.join(BASE_PATH, establo, "NACIMIENTOS")
        if not os.path.isdir(carpeta):
            continue
        for f in os.listdir(carpeta):
            if f.lower().endswith(".xlsx") and not f.startswith("~$"):
                presentes.add(f)
    return presentes

# ========= BORRADO POR FALTANTES =========
def borrar_faltantes_en_fs():
    indice = cargar_indice_existente()
    en_sql = set(indice.keys())
    en_fs  = escanear_archivos_fs()
    faltantes = sorted(en_sql - en_fs)
    if not faltantes:
        print("✔ No hay archivos eliminados en FS pendientes de borrar en dbo.NACIMIENTOS.")
        return
    print(f"🧹 Borrando {len(faltantes)} archivos inexistentes en FS…")
    with ENGINE.begin() as con:
        for nm in faltantes:
            con.execute(text("DELETE FROM dbo.NACIMIENTOS WHERE [Source.Name] = :nm"), {"nm": nm})
            print(f"   - eliminado en SQL: {nm}")

# ========= CARGA INCREMENTAL =========
def cargar_establos_incremental():
    indice = cargar_indice_existente()

    for establo in ESTABLOS:
        carpeta = os.path.join(BASE_PATH, establo, "NACIMIENTOS")
        if not os.path.isdir(carpeta):
            continue

        archivos = [f for f in os.listdir(carpeta)
                    if f.lower().endswith(".xlsx") and not f.startswith("~$")]

        for archivo in archivos:
            ruta = os.path.join(carpeta, archivo)
            try:
                mtime = pd.to_datetime(os.path.getmtime(ruta), unit="s")
                actual_ts = ts_str(mtime)
                ts_prev = indice.get(archivo)

                if ts_prev is not None and pd.to_datetime(actual_ts) <= pd.to_datetime(ts_prev):
                    print(f"⏩ Saltando {archivo} (sin cambios, mtime {actual_ts})")
                    continue

                if ts_prev is not None and pd.to_datetime(actual_ts) > pd.to_datetime(ts_prev):
                    with ENGINE.begin() as con:
                        con.execute(text("DELETE FROM dbo.NACIMIENTOS WHERE [Source.Name] = :nm"), {"nm": archivo})
                    print(f"🧹 Eliminado previo de {archivo} (TS previo {ts_prev})")

                # Leer excel SIN encabezado usando los nombres esperados
                df = pd.read_excel(ruta, header=None, names=COLUMNAS_EXCEL, engine="openpyxl")
                df.dropna(how="all", inplace=True)
                if df.empty:
                    print(f"⚠ {archivo} vacío, omitido.")
                    continue

                # Metadatos
                df["Source.Name"]     = archivo
                df["Source.FileDate"] = mtime.date()
                df["Source.FileTime"] = mtime.strftime("%H:%M:%S")

                # Normalizar tipos
                df = normalize_dataframe(df, establo)

                print(f"⏳ Cargando {archivo} desde {establo} ({actual_ts}) a dbo.NACIMIENTOS…")
                df.to_sql("NACIMIENTOS", ENGINE, if_exists="append", index=False, chunksize=1000, schema="dbo")
                print(f"✅ Cargado {archivo}.\n")

                indice[archivo] = actual_ts

            except Exception as e:
                print(f"❌ Error al cargar {archivo} desde {establo}: {e}\n")

if __name__ == "__main__":
    borrar_faltantes_en_fs()
    cargar_establos_incremental()

```

# 9)TEST_PREÑEZ

```python
import os
import urllib.parse
import pandas as pd
import numpy as np
from sqlalchemy import create_engine, text

# ========= CONFIG =========
SERVER   = "DESKTOP-3I6O45D"
DATABASE = "EVALUACION_ESTABLOS"
USER     = "sa"
PASSWORD = "123456789"

BASE_PATH = r"D:\PRUEBA PYTHON"
ESTABLOS = [
    "AGHOLSTEIN","ANGUS","ANTONIOJAIME","BALI","CANELAS","ESTRELLADEMANUEL",
    "GASUR","HUERTOS SAN MARTIN","LAREDO","MOCHERA","PIAMONTE","REMANSO",
    "SANTA FE","SAUSALITO","SEQUION"
]

TABLE_NAME = "TEST_DE_PRENEZ"
SUBFOLDER  = "TEST DE PREÑEZ"   # ajusta si tu carpeta tiene otro nombre

# === ORDEN EXACTO DE COLUMNAS A INSERTAR (SIN IDENTITY) ===
COLUMNAS_SQL = [
    "Source.Name","DairyName","FECHA_DE_ARCHIVO","COW","LC_ACTUAL","DIM_ACTUAL",
    "STAT_ACTUAL","RAZA","SEXO",
    "PrgVAIdCode","PrgNumLact","PrgCtrlCode","PrgOffC","PrgCom","PrgCom2",
    "PrgCount","PrgCntLf","PrgCost","PrgRevCode","PrgDim","PrgAge","PrgTech",
    "PrgDat","PrgTime","Prg2Do","PrgSidEffL2","PrgDiag","FECHA_DE_PARTO",
    "Source.FileDate","Source.FileTime"
]
# Columnas que vienen desde Excel (sin metadatos Source.*)
COLUMNAS_EXCEL = [c for c in COLUMNAS_SQL if not c.startswith("Source.")]

# ========= CONEXIÓN =========
def _make_engine(driver_name: str, encrypt_yes: bool):
    conn = (
        f"DRIVER={{{driver_name}}};SERVER={SERVER};DATABASE={DATABASE};UID={USER};PWD={PASSWORD};"
        f"TrustServerCertificate=Yes;Encrypt={'Yes' if encrypt_yes else 'No'};"
    )
    return create_engine(
        "mssql+pyodbc:///?odbc_connect=" + urllib.parse.quote_plus(conn),
        fast_executemany=True, pool_pre_ping=True
    )

def get_engine():
    for drv, enc in [("ODBC Driver 18 for SQL Server", True),
                     ("ODBC Driver 17 for SQL Server", False)]:
        try:
            eng = _make_engine(drv, enc)
            with eng.connect():
                pass
            print(f"Conectado con {drv}")
            return eng
        except Exception as e:
            print(f"No se pudo con {drv}: {e}")
    raise RuntimeError("No se pudo abrir conexión ODBC 17/18.")

ENGINE = get_engine()

# ========= NORMALIZACIONES =========
def to_date_series(s: pd.Series) -> pd.Series:
    return pd.to_datetime(s, errors="coerce", dayfirst=True).dt.date

def excel_days_to_time_from_series(s: pd.Series) -> pd.Series:
    td = pd.to_timedelta(pd.to_numeric(s, errors="coerce"), unit="D")
    return (pd.Timestamp("1899-12-30") + td).dt.time

def format_time_hms_series(tser: pd.Series) -> pd.Series:
    def fmt(x):
        if pd.isna(x):
            return None
        try:
            return x.strftime("%H:%M:%S")
        except Exception:
            return None
    return tser.apply(fmt)

def normalize_time(col: pd.Series) -> pd.Series:
    s = col.copy()
    # Excel serial o timedelta
    if pd.api.types.is_timedelta64_dtype(s):
        t = (pd.Timestamp("1900-01-01") + s).dt.time
        return format_time_hms_series(t)
    if pd.api.types.is_numeric_dtype(s):
        t = excel_days_to_time_from_series(s)
        return format_time_hms_series(t)
    # Texto u objeto datetime
    t = pd.to_datetime(s, errors="coerce").dt.time
    return format_time_hms_series(t)

def to_decimal_nullable(series: pd.Series) -> pd.Series:
    return pd.to_numeric(series, errors="coerce")

def cow_to_str(v):
    if pd.isna(v): return None
    if isinstance(v, (int, np.integer)): return str(int(v))
    if isinstance(v, float) and v.is_integer(): return str(int(v))
    return str(v)

def normalize_dataframe(df: pd.DataFrame, establo: str) -> pd.DataFrame:
    # Asegurar columnas y orden
    for c in COLUMNAS_SQL:
        if c not in df.columns:
            df[c] = np.nan
    df = df[COLUMNAS_SQL]

    # Defaults
    df["DairyName"] = df["DairyName"].fillna(establo)

    # Fechas
    for c in ["FECHA_DE_ARCHIVO", "PrgDat", "FECHA_DE_PARTO", "Source.FileDate"]:
        df[c] = to_date_series(df[c])

    # Horas
    for c in ["PrgTime", "Source.FileTime"]:
        df[c] = normalize_time(df[c])

    # Texto
    text_cols = [
        "Source.Name","DairyName","COW","STAT_ACTUAL","RAZA","SEXO",
        "PrgVAIdCode","PrgCtrlCode","PrgOffC","PrgCom","PrgCom2",
        "PrgRevCode","PrgTech","PrgSidEffL2","PrgDiag"
    ]
    for c in text_cols:
        df[c] = df[c].astype(str).where(df[c].notna(), None)

    # cow como string sin .0
    df["COW"] = df["COW"].apply(cow_to_str)

    # Numéricos enteros
    int_cols = [
        "LC_ACTUAL","DIM_ACTUAL","PrgNumLact","PrgCount","PrgCntLf","PrgDim","Prg2Do"
    ]
    for c in int_cols:
        df[c] = pd.to_numeric(df[c], errors="coerce").astype("Int64")

    # Numéricos decimales
    for c in ["PrgCost","PrgAge"]:
        df[c] = to_decimal_nullable(df[c])

    # Timedelta defensivo
    for c in df.columns:
        if pd.api.types.is_timedelta64_dtype(df[c]):
            df[c] = format_time_hms_series((pd.Timestamp("1900-01-01") + df[c]).dt.time)

    return df

def ts_str(dt: pd.Timestamp) -> str:
    return dt.strftime("%Y-%m-%d %H:%M:%S")

# ========= ÍNDICE SQL + FS =========
def cargar_indice_existente() -> dict:
    try:
        q = f"""
        SELECT [Source.Name] AS SName,
               CONVERT(varchar(10), [Source.FileDate], 120) + ' ' +
               CONVERT(varchar(8),  [Source.FileTime], 108) AS STS
        FROM dbo.{TABLE_NAME}
        WHERE [Source.Name] IS NOT NULL AND [Source.FileDate] IS NOT NULL AND [Source.FileTime] IS NOT NULL
        GROUP BY [Source.Name], [Source.FileDate], [Source.FileTime]
        """
        df = pd.read_sql_query(q, ENGINE)
        return (df.assign(dt=pd.to_datetime(df["STS"], errors="coerce"))
                  .dropna(subset=["dt"])
                  .sort_values(["SName","dt"])
                  .groupby("SName")["STS"].last()
                  .to_dict())
    except Exception as e:
        print(f"No se pudo leer índice existente: {e}")
        return {}

def escanear_archivos_fs() -> set:
    presentes = set()
    for establo in ESTABLOS:
        carpeta = os.path.join(BASE_PATH, establo, SUBFOLDER)
        if not os.path.isdir(carpeta):
            continue
        for f in os.listdir(carpeta):
            if f.lower().endswith(".xlsx") and not f.startswith("~$"):
                presentes.add(f)
    return presentes

# ========= BORRADO POR FALTANTES =========
def borrar_faltantes_en_fs():
    indice = cargar_indice_existente()
    en_sql = set(indice.keys())
    en_fs  = escanear_archivos_fs()
    faltantes = sorted(en_sql - en_fs)
    if not faltantes:
        print(f"✔ No hay archivos eliminados en FS pendientes de borrar en dbo.{TABLE_NAME}.")
        return
    print(f"🧹 Borrando {len(faltantes)} archivos inexistentes en FS…")
    with ENGINE.begin() as con:
        for nm in faltantes:
            con.execute(text(f"DELETE FROM dbo.{TABLE_NAME} WHERE [Source.Name] = :nm"), {"nm": nm})
            print(f"   - eliminado en SQL: {nm}")

# ========= CARGA INCREMENTAL =========
def cargar_establos_incremental():
    indice = cargar_indice_existente()

    for establo in ESTABLOS:
        carpeta = os.path.join(BASE_PATH, establo, SUBFOLDER)
        if not os.path.isdir(carpeta):
            continue

        archivos = [f for f in os.listdir(carpeta)
                    if f.lower().endswith(".xlsx") and not f.startswith("~$")]

        for archivo in archivos:
            ruta = os.path.join(carpeta, archivo)
            try:
                mtime = pd.to_datetime(os.path.getmtime(ruta), unit="s")
                actual_ts = ts_str(mtime)
                ts_prev = indice.get(archivo)

                if ts_prev is not None and pd.to_datetime(actual_ts) <= pd.to_datetime(ts_prev):
                    print(f"⏩ Saltando {archivo} (sin cambios, mtime {actual_ts})")
                    continue

                if ts_prev is not None and pd.to_datetime(actual_ts) > pd.to_datetime(ts_prev):
                    with ENGINE.begin() as con:
                        con.execute(text(f"DELETE FROM dbo.{TABLE_NAME} WHERE [Source.Name] = :nm"), {"nm": archivo})
                    print(f"🧹 Eliminado previo de {archivo} (TS previo {ts_prev})")

                # Leer excel SIN encabezado con nombres esperados
                df = pd.read_excel(ruta, header=None, names=COLUMNAS_EXCEL, engine="openpyxl")
                df.dropna(how="all", inplace=True)
                if df.empty:
                    print(f"⚠ {archivo} vacío, omitido.")
                    continue

                # Metadatos
                df["Source.Name"]     = archivo
                df["Source.FileDate"] = mtime.date()
                df["Source.FileTime"] = mtime.strftime("%H:%M:%S")

                # Normalizar tipos
                df = normalize_dataframe(df, establo)

                print(f"⏳ Cargando {archivo} desde {establo} ({actual_ts}) a dbo.{TABLE_NAME}…")
                df.to_sql(TABLE_NAME, ENGINE, if_exists="append", index=False, chunksize=1000, schema="dbo")
                print(f"✅ Cargado {archivo}.\n")

                indice[archivo] = actual_ts

            except Exception as e:
                print(f"❌ Error al cargar {archivo} desde {establo}: {e}\n")

if __name__ == "__main__":
    borrar_faltantes_en_fs()
    cargar_establos_incremental()

```

# 10) LACTACIONES

```python
import os
import urllib.parse
import pandas as pd
import numpy as np
from sqlalchemy import create_engine, text

# ========= CONFIG =========
SERVER   = "DESKTOP-3I6O45D"
DATABASE = "EVALUACION_ESTABLOS"
USER     = "sa"
PASSWORD = "123456789"

BASE_PATH = r"D:\PRUEBA PYTHON"
ESTABLOS = [
    "AGHOLSTEIN","ANGUS","ANTONIOJAIME","BALI","CANELAS","ESTRELLADEMANUEL",
    "GASUR","HUERTOS SAN MARTIN","LAREDO","MOCHERA","PIAMONTE","REMANSO",
    "SANTA FE","SAUSALITO","SEQUION"
]

TABLE_NAME = "LACTACIONES"
SUBFOLDER  = "LACTACIONES"

# === ORDEN EXACTO DE COLUMNAS A INSERTAR (SIN IDENTITY) ===
COLUMNAS_SQL = [
    "Source.Name","DairyName","FECHA_ARCHIVO","COW","STAT_ACTUAL","PDCalve",
    "LactNo","LDim","Calf1","Calf2","DFC","FC","MC","DMC","DC",
    "LactM","M100","M200","M305","CalfNum","Calf2Num","CalfSex2","CalfSex","CI",
    "Source.FileDate","Source.FileTime"
]
# Columnas que llegan desde Excel (sin metadatos Source.*)
COLUMNAS_EXCEL = [c for c in COLUMNAS_SQL if not c.startswith("Source.")]

# ========= CONEXIÓN =========
def _make_engine(driver_name: str, encrypt_yes: bool):
    conn = (
        f"DRIVER={{{driver_name}}};SERVER={SERVER};DATABASE={DATABASE};UID={USER};PWD={PASSWORD};"
        f"TrustServerCertificate=Yes;Encrypt={'Yes' if encrypt_yes else 'No'};"
    )
    return create_engine(
        "mssql+pyodbc:///?odbc_connect=" + urllib.parse.quote_plus(conn),
        fast_executemany=True, pool_pre_ping=True
    )

def get_engine():
    for drv, enc in [("ODBC Driver 18 for SQL Server", True),
                     ("ODBC Driver 17 for SQL Server", False)]:
        try:
            eng = _make_engine(drv, enc)
            with eng.connect():
                pass
            print(f"Conectado con {drv}")
            return eng
        except Exception as e:
            print(f"No se pudo con {drv}: {e}")
    raise RuntimeError("No se pudo abrir conexión ODBC 17/18.")

ENGINE = get_engine()

# ========= NORMALIZACIONES =========
def to_date_series(s: pd.Series) -> pd.Series:
    return pd.to_datetime(s, errors="coerce", dayfirst=True).dt.date

def excel_days_to_time_from_series(s: pd.Series) -> pd.Series:
    td = pd.to_timedelta(pd.to_numeric(s, errors="coerce"), unit="D")
    return (pd.Timestamp("1899-12-30") + td).dt.time

def format_time_hms_series(tser: pd.Series) -> pd.Series:
    def fmt(x):
        if pd.isna(x):
            return None
        try:
            return x.strftime("%H:%M:%S")
        except Exception:
            return None
    return tser.apply(fmt)

def normalize_time(col: pd.Series) -> pd.Series:
    s = col.copy()
    if pd.api.types.is_timedelta64_dtype(s):
        t = (pd.Timestamp("1900-01-01") + s).dt.time
        return format_time_hms_series(t)
    if pd.api.types.is_numeric_dtype(s):
        t = excel_days_to_time_from_series(s)
        return format_time_hms_series(t)
    t = pd.to_datetime(s, errors="coerce").dt.time
    return format_time_hms_series(t)

def cow_to_str(v):
    if pd.isna(v): return None
    if isinstance(v, (int, np.integer)): return str(int(v))
    if isinstance(v, float) and v.is_integer(): return str(int(v))
    return str(v)

def normalize_dataframe(df: pd.DataFrame, establo: str) -> pd.DataFrame:
    # forzar columnas y orden
    for c in COLUMNAS_SQL:
        if c not in df.columns:
            df[c] = np.nan
    df = df[COLUMNAS_SQL]

    # defaults
    df["DairyName"] = df["DairyName"].fillna(establo)

    # fechas
    for c in ["FECHA_ARCHIVO","Source.FileDate"]:
        df[c] = to_date_series(df[c])

    # hora
    df["Source.FileTime"] = normalize_time(df["Source.FileTime"])

    # texto
    for c in ["Source.Name","DairyName","COW","STAT_ACTUAL","PDCalve","Calf1","Calf2","CalfSex2","CalfSex"]:
        df[c] = df[c].astype(str).where(df[c].notna(), None)

    # cow sin .0
    df["COW"] = df["COW"].apply(cow_to_str)

    # enteros
    for c in ["LactNo","LDim","DFC","CalfNum","Calf2Num", "FC","MC","DMC","DC"]:
        df[c] = pd.to_numeric(df[c], errors="coerce").astype("Int64")

    # decimales
    for c in ["LactM","M100","M200","M305","CI"]:
        df[c] = pd.to_numeric(df[c], errors="coerce")

    # defensivo: columnas timedelta a HH:MM:SS
    for c in df.columns:
        if pd.api.types.is_timedelta64_dtype(df[c]):
            df[c] = format_time_hms_series((pd.Timestamp("1900-01-01") + df[c]).dt.time)

    return df

def ts_str(dt: pd.Timestamp) -> str:
    return dt.strftime("%Y-%m-%d %H:%M:%S")

# ========= ÍNDICE SQL + FS =========
def cargar_indice_existente() -> dict:
    try:
        q = f"""
        SELECT [Source.Name] AS SName,
               CONVERT(varchar(10), [Source.FileDate], 120) + ' ' +
               CONVERT(varchar(8),  [Source.FileTime], 108) AS STS
        FROM dbo.{TABLE_NAME}
        WHERE [Source.Name] IS NOT NULL AND [Source.FileDate] IS NOT NULL AND [Source.FileTime] IS NOT NULL
        GROUP BY [Source.Name], [Source.FileDate], [Source.FileTime]
        """
        df = pd.read_sql_query(q, ENGINE)
        return (df.assign(dt=pd.to_datetime(df["STS"], errors="coerce"))
                  .dropna(subset=["dt"])
                  .sort_values(["SName","dt"])
                  .groupby("SName")["STS"].last()
                  .to_dict())
    except Exception as e:
        print(f"No se pudo leer índice existente: {e}")
        return {}

def escanear_archivos_fs() -> set:
    presentes = set()
    for establo in ESTABLOS:
        carpeta = os.path.join(BASE_PATH, establo, SUBFOLDER)
        if not os.path.isdir(carpeta):
            continue
        for f in os.listdir(carpeta):
            if f.lower().endswith(".xlsx") and not f.startswith("~$"):
                presentes.add(f)
    return presentes

# ========= BORRADO POR FALTANTES =========
def borrar_faltantes_en_fs():
    indice = cargar_indice_existente()
    en_sql = set(indice.keys())
    en_fs  = escanear_archivos_fs()
    faltantes = sorted(en_sql - en_fs)
    if not faltantes:
        print(f"✔ No hay archivos eliminados en FS pendientes de borrar en dbo.{TABLE_NAME}.")
        return
    print(f"🧹 Borrando {len(faltantes)} archivos inexistentes en FS…")
    with ENGINE.begin() as con:
        for nm in faltantes:
            con.execute(text(f"DELETE FROM dbo.{TABLE_NAME} WHERE [Source.Name] = :nm"), {"nm": nm})
            print(f"   - eliminado en SQL: {nm}")

# ========= CARGA INCREMENTAL =========
def cargar_establos_incremental():
    indice = cargar_indice_existente()

    for establo in ESTABLOS:
        carpeta = os.path.join(BASE_PATH, establo, SUBFOLDER)
        if not os.path.isdir(carpeta):
            continue

        archivos = [f for f in os.listdir(carpeta)
                    if f.lower().endswith(".xlsx") and not f.startswith("~$")]

        for archivo in archivos:
            ruta = os.path.join(carpeta, archivo)
            try:
                mtime = pd.to_datetime(os.path.getmtime(ruta), unit="s")
                actual_ts = ts_str(mtime)
                ts_prev = indice.get(archivo)

                if ts_prev is not None and pd.to_datetime(actual_ts) <= pd.to_datetime(ts_prev):
                    print(f"⏩ Saltando {archivo} (sin cambios, mtime {actual_ts})")
                    continue

                if ts_prev is not None and pd.to_datetime(actual_ts) > pd.to_datetime(ts_prev):
                    with ENGINE.begin() as con:
                        con.execute(text(f"DELETE FROM dbo.{TABLE_NAME} WHERE [Source.Name] = :nm"), {"nm": archivo})
                    print(f"🧹 Eliminado previo de {archivo} (TS previo {ts_prev})")

                # leer excel SIN encabezado, asignando nombres esperados
                df = pd.read_excel(ruta, header=None, names=COLUMNAS_EXCEL, engine="openpyxl")
                df.dropna(how="all", inplace=True)
                if df.empty:
                    print(f"⚠ {archivo} vacío, omitido.")
                    continue

                # metadatos
                df["Source.Name"]     = archivo
                df["Source.FileDate"] = mtime.date()
                df["Source.FileTime"] = mtime.strftime("%H:%M:%S")

                # normalizar tipos
                df = normalize_dataframe(df, establo)

                print(f"⏳ Cargando {archivo} desde {establo} ({actual_ts}) a dbo.{TABLE_NAME}…")
                df.to_sql(TABLE_NAME, ENGINE, if_exists="append", index=False, chunksize=1000, schema="dbo")
                print(f"✅ Cargado {archivo}.\n")

                indice[archivo] = actual_ts

            except Exception as e:
                print(f"❌ Error al cargar {archivo} desde {establo}: {e}\n")

if __name__ == "__main__":
    borrar_faltantes_en_fs()
    cargar_establos_incremental()
```

# 11) GENERAL RECRIA

```python
import os
import urllib.parse
import pandas as pd
import numpy as np
from sqlalchemy import create_engine, text

# ========= CONFIG =========
SERVER   = "DESKTOP-3I6O45D"
DATABASE = "EVALUACION_ESTABLOS"
USER     = "sa"
PASSWORD = "123456789"

BASE_PATH = r"D:\PRUEBA PYTHON"
ESTABLOS = [
    "AGHOLSTEIN","ANGUS","ANTONIOJAIME","BALI","CANELAS","ESTRELLADEMANUEL",
    "GASUR","HUERTOS SAN MARTIN","LAREDO","MOCHERA","PIAMONTE","REMANSO",
    "SANTA FE","SAUSALITO","SEQUION"
]

TABLE_NAME = "GENERALES_RECRIA"
SUBFOLDER  = "GENERALES RECRIA"

# === ORDEN EXACTO DE COLUMNAS A INSERTAR (SIN IDENTITY) ===
COLUMNAS_SQL = [
    "Source.Name","DairyName","today()","HSDate",
    "HSAnimalCountTotal.0@YF","HSCountAnimElig.0@Y",
    "HSInsemCountTotal@Y","HSInsemCountSuccess@Y",
    "HSInsemRateCycle@Y","HSInsemSuccess@Y","HSPregRateCycle@Y",
    "HSCountPreg@Y","HSAnimalCountLeft@yF",
    "Source.FileDate","Source.FileTime"
]
COLUMNAS_EXCEL = [c for c in COLUMNAS_SQL if not c.startswith("Source.")]

# ========= CONEXIÓN =========
def _make_engine(driver_name: str, encrypt_yes: bool):
    conn = (
        f"DRIVER={{{driver_name}}};SERVER={SERVER};DATABASE={DATABASE};UID={USER};PWD={PASSWORD};"
        f"TrustServerCertificate=Yes;Encrypt={'Yes' if encrypt_yes else 'No'};"
    )
    return create_engine(
        "mssql+pyodbc:///?odbc_connect=" + urllib.parse.quote_plus(conn),
        fast_executemany=True, pool_pre_ping=True
    )

def get_engine():
    for drv, enc in [("ODBC Driver 18 for SQL Server", True),
                     ("ODBC Driver 17 for SQL Server", False)]:
        try:
            eng = _make_engine(drv, enc)
            with eng.connect():
                pass
            print(f"Conectado con {drv}")
            return eng
        except Exception as e:
            print(f"No se pudo con {drv}: {e}")
    raise RuntimeError("No se pudo abrir conexión ODBC 17/18.")

ENGINE = get_engine()

# ========= NORMALIZACIONES =========
def to_date_series(s: pd.Series) -> pd.Series:
    return pd.to_datetime(s, errors="coerce", dayfirst=True).dt.date

def excel_days_to_time_from_series(s: pd.Series) -> pd.Series:
    td = pd.to_timedelta(pd.to_numeric(s, errors="coerce"), unit="D")
    return (pd.Timestamp("1899-12-30") + td).dt.time

def format_time_hms_series(tser: pd.Series) -> pd.Series:
    def fmt(x):
        if pd.isna(x): return None
        try: return x.strftime("%H:%M:%S")
        except Exception: return None
    return tser.apply(fmt)

def normalize_time(col: pd.Series) -> pd.Series:
    s = col.copy()
    if pd.api.types.is_timedelta64_dtype(s):
        t = (pd.Timestamp("1900-01-01") + s).dt.time
        return format_time_hms_series(t)
    if pd.api.types.is_numeric_dtype(s):
        t = excel_days_to_time_from_series(s)
        return format_time_hms_series(t)
    t = pd.to_datetime(s, errors="coerce").dt.time
    return format_time_hms_series(t)

INT_COLS = ["HSAnimalCountLeft@yF"]
FLOAT_COLS = [
    "HSAnimalCountTotal.0@YF","HSCountAnimElig.0@Y",
    "HSInsemCountTotal@Y","HSInsemCountSuccess@Y",
    "HSInsemRateCycle@Y","HSInsemSuccess@Y",
    "HSPregRateCycle@Y","HSCountPreg@Y"
]

def normalize_dataframe(df: pd.DataFrame, establo: str) -> pd.DataFrame:
    # garantizar columnas y orden
    for c in COLUMNAS_SQL:
        if c not in df.columns:
            df[c] = np.nan
    df = df[COLUMNAS_SQL]

    # textos
    df["DairyName"] = df["DairyName"].fillna(establo)
    for c in ["Source.Name","DairyName"]:
        df[c] = df[c].astype(str).where(df[c].notna(), None)

    # fechas y hora
    for c in ["today()","HSDate","Source.FileDate"]:
        df[c] = to_date_series(df[c])
    df["Source.FileTime"] = normalize_time(df["Source.FileTime"])

    # enteros (redondeo si llegan decimales)
    for c in INT_COLS:
        s = pd.to_numeric(df[c], errors="coerce")
        if (s.dropna() % 1 != 0).any():
            print(f"⚠ {c}: contiene decimales. Se redondeará.")
            s = s.round(0)
        df[c] = s.astype("Int64")

    # decimales
    for c in FLOAT_COLS:
        df[c] = pd.to_numeric(df[c], errors="coerce")

    return df

def ts_str(dt: pd.Timestamp) -> str:
    return dt.strftime("%Y-%m-%d %H:%M:%S")

# ========= ÍNDICE SQL + FS =========
def cargar_indice_existente() -> dict:
    try:
        q = f"""
        SELECT [Source.Name] AS SName,
               CONVERT(varchar(10), [Source.FileDate], 120) + ' ' +
               CONVERT(varchar(8),  [Source.FileTime], 108) AS STS
        FROM dbo.{TABLE_NAME}
        WHERE [Source.Name] IS NOT NULL AND [Source.FileDate] IS NOT NULL AND [Source.FileTime] IS NOT NULL
        GROUP BY [Source.Name], [Source.FileDate], [Source.FileTime]
        """
        df = pd.read_sql_query(q, ENGINE)
        return (df.assign(dt=pd.to_datetime(df["STS"], errors="coerce"))
                  .dropna(subset=["dt"])
                  .sort_values(["SName","dt"])
                  .groupby("SName")["STS"].last()
                  .to_dict())
    except Exception as e:
        print(f"No se pudo leer índice existente: {e}")
        return {}

def escanear_archivos_fs() -> set:
    presentes = set()
    for establo in ESTABLOS:
        carpeta = os.path.join(BASE_PATH, establo, SUBFOLDER)
        if not os.path.isdir(carpeta):
            continue
        for f in os.listdir(carpeta):
            if f.lower().endswith(".xlsx") and not f.startswith("~$"):
                presentes.add(f)
    return presentes

# ========= BORRADO POR FALTANTES =========
def borrar_faltantes_en_fs():
    indice = cargar_indice_existente()
    en_sql = set(indice.keys())
    en_fs  = escanear_archivos_fs()
    faltantes = sorted(en_sql - en_fs)
    if not faltantes:
        print(f"✔ No hay archivos eliminados en FS pendientes de borrar en dbo.{TABLE_NAME}.")
        return
    print(f"🧹 Borrando {len(faltantes)} archivos inexistentes en FS…")
    with ENGINE.begin() as con:
        for nm in faltantes:
            con.execute(text(f"DELETE FROM dbo.{TABLE_NAME} WHERE [Source.Name] = :nm"), {"nm": nm})
            print(f"   - eliminado en SQL: {nm}")

# ========= CARGA INCREMENTAL =========
def cargar_establos_incremental():
    indice = cargar_indice_existente()

    for establo in ESTABLOS:
        carpeta = os.path.join(BASE_PATH, establo, SUBFOLDER)
        if not os.path.isdir(carpeta):
            continue

        archivos = [f for f in os.listdir(carpeta)
                    if f.lower().endswith(".xlsx") and not f.startswith("~$")]

        for archivo in archivos:
            ruta = os.path.join(carpeta, archivo)
            try:
                mtime = pd.to_datetime(os.path.getmtime(ruta), unit="s")
                actual_ts = ts_str(mtime)
                ts_prev = indice.get(archivo)

                if ts_prev is not None and pd.to_datetime(actual_ts) <= pd.to_datetime(ts_prev):
                    print(f"⏩ Saltando {archivo} (sin cambios, mtime {actual_ts})")
                    continue

                if ts_prev is not None and pd.to_datetime(actual_ts) > pd.to_datetime(ts_prev):
                    with ENGINE.begin() as con:
                        con.execute(text(f"DELETE FROM dbo.{TABLE_NAME} WHERE [Source.Name] = :nm"), {"nm": archivo})
                    print(f"🧹 Eliminado previo de {archivo} (TS previo {ts_prev})")

                # leer excel SIN encabezado, asignando nombres esperados
                df = pd.read_excel(ruta, header=None, names=COLUMNAS_EXCEL, engine="openpyxl")
                df.dropna(how="all", inplace=True)
                if df.empty:
                    print(f"⚠ {archivo} vacío, omitido.")
                    continue

                # metadatos
                df["Source.Name"]     = archivo
                df["Source.FileDate"] = mtime.date()
                df["Source.FileTime"] = mtime.strftime("%H:%M:%S")

                # normalizar tipos
                df = normalize_dataframe(df, establo)

                print(f"⏳ Cargando {archivo} desde {establo} ({actual_ts}) a dbo.{TABLE_NAME}…")
                df.to_sql(TABLE_NAME, ENGINE, if_exists="append", index=False, chunksize=1000, schema="dbo")
                print(f"✅ Cargado {archivo}.\n")

                indice[archivo] = actual_ts

            except Exception as e:
                print(f"❌ Error al cargar {archivo} desde {establo}: {e}\n")

if __name__ == "__main__":
    borrar_faltantes_en_fs()
    cargar_establos_incremental()

```

# 12) GENERAL ESTABLOS

```python
import os
import urllib.parse
import pandas as pd
import numpy as np
from sqlalchemy import create_engine, text

# ========= CONFIG =========
SERVER   = "DESKTOP-3I6O45D"
DATABASE = "EVALUACION_ESTABLOS"
USER     = "sa"
PASSWORD = "123456789"

BASE_PATH = r"D:\PRUEBA PYTHON"
ESTABLOS = [
    "AGHOLSTEIN","ANGUS","ANTONIOJAIME","BALI","CANELAS","ESTRELLADEMANUEL",
    "GASUR","HUERTOS SAN MARTIN","LAREDO","MOCHERA","PIAMONTE","REMANSO",
    "SANTA FE","SAUSALITO","SEQUION"
]

TABLE_NAME = "GENERALES_RECRIA"
SUBFOLDER  = "GENERALES RECRIA"

# === ORDEN EXACTO DE COLUMNAS A INSERTAR (SIN IDENTITY) ===
COLUMNAS_SQL = [
    "Source.Name","DairyName","today()","HSDate",
    "HSAnimalCountTotal.0@YF","HSCountAnimElig.0@Y",
    "HSInsemCountTotal@Y","HSInsemCountSuccess@Y",
    "HSInsemRateCycle@Y","HSInsemSuccess@Y","HSPregRateCycle@Y",
    "HSCountPreg@Y","HSAnimalCountLeft@yF",
    "Source.FileDate","Source.FileTime"
]
COLUMNAS_EXCEL = [c for c in COLUMNAS_SQL if not c.startswith("Source.")]

# ========= CONEXIÓN =========
def _make_engine(driver_name: str, encrypt_yes: bool):
    conn = (
        f"DRIVER={{{driver_name}}};SERVER={SERVER};DATABASE={DATABASE};UID={USER};PWD={PASSWORD};"
        f"TrustServerCertificate=Yes;Encrypt={'Yes' if encrypt_yes else 'No'};"
    )
    return create_engine(
        "mssql+pyodbc:///?odbc_connect=" + urllib.parse.quote_plus(conn),
        fast_executemany=True, pool_pre_ping=True
    )

def get_engine():
    for drv, enc in [("ODBC Driver 18 for SQL Server", True),
                     ("ODBC Driver 17 for SQL Server", False)]:
        try:
            eng = _make_engine(drv, enc)
            with eng.connect():
                pass
            print(f"Conectado con {drv}")
            return eng
        except Exception as e:
            print(f"No se pudo con {drv}: {e}")
    raise RuntimeError("No se pudo abrir conexión ODBC 17/18.")

ENGINE = get_engine()

# ========= NORMALIZACIONES =========
def to_date_series(s: pd.Series) -> pd.Series:
    return pd.to_datetime(s, errors="coerce", dayfirst=True).dt.date

def excel_days_to_time_from_series(s: pd.Series) -> pd.Series:
    td = pd.to_timedelta(pd.to_numeric(s, errors="coerce"), unit="D")
    return (pd.Timestamp("1899-12-30") + td).dt.time

def format_time_hms_series(tser: pd.Series) -> pd.Series:
    def fmt(x):
        if pd.isna(x): return None
        try: return x.strftime("%H:%M:%S")
        except Exception: return None
    return tser.apply(fmt)

def normalize_time(col: pd.Series) -> pd.Series:
    s = col.copy()
    if pd.api.types.is_timedelta64_dtype(s):
        t = (pd.Timestamp("1900-01-01") + s).dt.time
        return format_time_hms_series(t)
    if pd.api.types.is_numeric_dtype(s):
        t = excel_days_to_time_from_series(s)
        return format_time_hms_series(t)
    t = pd.to_datetime(s, errors="coerce").dt.time
    return format_time_hms_series(t)

INT_COLS = ["HSAnimalCountLeft@yF"]
FLOAT_COLS = [
    "HSAnimalCountTotal.0@YF","HSCountAnimElig.0@Y",
    "HSInsemCountTotal@Y","HSInsemCountSuccess@Y",
    "HSInsemRateCycle@Y","HSInsemSuccess@Y",
    "HSPregRateCycle@Y","HSCountPreg@Y"
]

def normalize_dataframe(df: pd.DataFrame, establo: str) -> pd.DataFrame:
    # garantizar columnas y orden
    for c in COLUMNAS_SQL:
        if c not in df.columns:
            df[c] = np.nan
    df = df[COLUMNAS_SQL]

    # textos
    df["DairyName"] = df["DairyName"].fillna(establo)
    for c in ["Source.Name","DairyName"]:
        df[c] = df[c].astype(str).where(df[c].notna(), None)

    # fechas y hora
    for c in ["today()","HSDate","Source.FileDate"]:
        df[c] = to_date_series(df[c])
    df["Source.FileTime"] = normalize_time(df["Source.FileTime"])

    # enteros (redondeo si llegan decimales)
    for c in INT_COLS:
        s = pd.to_numeric(df[c], errors="coerce")
        if (s.dropna() % 1 != 0).any():
            print(f"⚠ {c}: contiene decimales. Se redondeará.")
            s = s.round(0)
        df[c] = s.astype("Int64")

    # decimales
    for c in FLOAT_COLS:
        df[c] = pd.to_numeric(df[c], errors="coerce")

    return df

def ts_str(dt: pd.Timestamp) -> str:
    return dt.strftime("%Y-%m-%d %H:%M:%S")

# ========= ÍNDICE SQL + FS =========
def cargar_indice_existente() -> dict:
    try:
        q = f"""
        SELECT [Source.Name] AS SName,
               CONVERT(varchar(10), [Source.FileDate], 120) + ' ' +
               CONVERT(varchar(8),  [Source.FileTime], 108) AS STS
        FROM dbo.{TABLE_NAME}
        WHERE [Source.Name] IS NOT NULL AND [Source.FileDate] IS NOT NULL AND [Source.FileTime] IS NOT NULL
        GROUP BY [Source.Name], [Source.FileDate], [Source.FileTime]
        """
        df = pd.read_sql_query(q, ENGINE)
        return (df.assign(dt=pd.to_datetime(df["STS"], errors="coerce"))
                  .dropna(subset=["dt"])
                  .sort_values(["SName","dt"])
                  .groupby("SName")["STS"].last()
                  .to_dict())
    except Exception as e:
        print(f"No se pudo leer índice existente: {e}")
        return {}

def escanear_archivos_fs() -> set:
    presentes = set()
    for establo in ESTABLOS:
        carpeta = os.path.join(BASE_PATH, establo, SUBFOLDER)
        if not os.path.isdir(carpeta):
            continue
        for f in os.listdir(carpeta):
            if f.lower().endswith(".xlsx") and not f.startswith("~$"):
                presentes.add(f)
    return presentes

# ========= BORRADO POR FALTANTES =========
def borrar_faltantes_en_fs():
    indice = cargar_indice_existente()
    en_sql = set(indice.keys())
    en_fs  = escanear_archivos_fs()
    faltantes = sorted(en_sql - en_fs)
    if not faltantes:
        print(f"✔ No hay archivos eliminados en FS pendientes de borrar en dbo.{TABLE_NAME}.")
        return
    print(f"🧹 Borrando {len(faltantes)} archivos inexistentes en FS…")
    with ENGINE.begin() as con:
        for nm in faltantes:
            con.execute(text(f"DELETE FROM dbo.{TABLE_NAME} WHERE [Source.Name] = :nm"), {"nm": nm})
            print(f"   - eliminado en SQL: {nm}")

# ========= CARGA INCREMENTAL =========
def cargar_establos_incremental():
    indice = cargar_indice_existente()

    for establo in ESTABLOS:
        carpeta = os.path.join(BASE_PATH, establo, SUBFOLDER)
        if not os.path.isdir(carpeta):
            continue

        archivos = [f for f in os.listdir(carpeta)
                    if f.lower().endswith(".xlsx") and not f.startswith("~$")]

        for archivo in archivos:
            ruta = os.path.join(carpeta, archivo)
            try:
                mtime = pd.to_datetime(os.path.getmtime(ruta), unit="s")
                actual_ts = ts_str(mtime)
                ts_prev = indice.get(archivo)

                if ts_prev is not None and pd.to_datetime(actual_ts) <= pd.to_datetime(ts_prev):
                    print(f"⏩ Saltando {archivo} (sin cambios, mtime {actual_ts})")
                    continue

                if ts_prev is not None and pd.to_datetime(actual_ts) > pd.to_datetime(ts_prev):
                    with ENGINE.begin() as con:
                        con.execute(text(f"DELETE FROM dbo.{TABLE_NAME} WHERE [Source.Name] = :nm"), {"nm": archivo})
                    print(f"🧹 Eliminado previo de {archivo} (TS previo {ts_prev})")

                # leer excel SIN encabezado, asignando nombres esperados
                df = pd.read_excel(ruta, header=None, names=COLUMNAS_EXCEL, engine="openpyxl")
                df.dropna(how="all", inplace=True)
                if df.empty:
                    print(f"⚠ {archivo} vacío, omitido.")
                    continue

                # metadatos
                df["Source.Name"]     = archivo
                df["Source.FileDate"] = mtime.date()
                df["Source.FileTime"] = mtime.strftime("%H:%M:%S")

                # normalizar tipos
                df = normalize_dataframe(df, establo)

                print(f"⏳ Cargando {archivo} desde {establo} ({actual_ts}) a dbo.{TABLE_NAME}…")
                df.to_sql(TABLE_NAME, ENGINE, if_exists="append", index=False, chunksize=1000, schema="dbo")
                print(f"✅ Cargado {archivo}.\n")

                indice[archivo] = actual_ts

            except Exception as e:
                print(f"❌ Error al cargar {archivo} desde {establo}: {e}\n")

if __name__ == "__main__":
    borrar_faltantes_en_fs()
    cargar_establos_incremental()

```

# 13)GENERAL VACAS

```python
import os
import urllib.parse
import pandas as pd
import numpy as np
from sqlalchemy import create_engine, text

# ========= CONFIG =========
SERVER   = "DESKTOP-3I6O45D"
DATABASE = "EVALUACION_ESTABLOS"
USER     = "sa"
PASSWORD = "123456789"

BASE_PATH = r"D:\PRUEBA PYTHON"
ESTABLOS = [
    "AGHOLSTEIN","ANGUS","ANTONIOJAIME","BALI","CANELAS","ESTRELLADEMANUEL",
    "GASUR","HUERTOS SAN MARTIN","LAREDO","MOCHERA","PIAMONTE","REMANSO",
    "SANTA FE","SAUSALITO","SEQUION"
]

TABLE_NAME = "GENERALES_VACAS"
SUBFOLDER  = "GENERALES VACAS"

# === ORDEN EXACTO DE COLUMNAS A INSERTAR (SIN IDENTITY) ===
COLUMNAS_SQL = [
    "Source.Name","DairyName","today()","HSDate",
    "HSAnimalCountTotal.0@1p","HSAnimalCountMilking.0","HSAnimalCountDry.0@1p","HSCountAnimElig.0",
    "HSInsemCountTotal@1p","HSInsemCountSuccess@1p","HSInsemRateCycle@1p","HSInsemSuccess@1p","HSPregRateCycle@1p",
    "HSAvgDIMFirstInsem@1p","HSAvgDaysOpen@1p","HSCountPreg@1p.0","HSAvgCalvingInterval",
    "HSAvgPeakMilk@1p","HSAvgPeakMilkDIM","HSAvgDailyMilk",
    "HSRollingHerdAvg@1p","HSRollingHerdAvg@1","HSRollingHerdAvg@2p",
    "HSAnimalCountLeft@1p",
    "Source.FileDate","Source.FileTime"
]
COLUMNAS_EXCEL = [c for c in COLUMNAS_SQL if not c.startswith("Source.")]

# ========= CONEXIÓN =========
def _make_engine(driver_name: str, encrypt_yes: bool):
    conn = (
        f"DRIVER={{{driver_name}}};SERVER={SERVER};DATABASE={DATABASE};UID={USER};PWD={PASSWORD};"
        f"TrustServerCertificate=Yes;Encrypt={'Yes' if encrypt_yes else 'No'};"
    )
    return create_engine(
        "mssql+pyodbc:///?odbc_connect=" + urllib.parse.quote_plus(conn),
        fast_executemany=True, pool_pre_ping=True
    )

def get_engine():
    for drv, enc in [("ODBC Driver 18 for SQL Server", True),
                     ("ODBC Driver 17 for SQL Server", False)]:
        try:
            eng = _make_engine(drv, enc)
            with eng.connect():
                pass
            print(f"Conectado con {drv}")
            return eng
        except Exception as e:
            print(f"No se pudo con {drv}: {e}")
    raise RuntimeError("No se pudo abrir conexión ODBC 17/18.")

ENGINE = get_engine()

# ========= NORMALIZACIONES =========
def to_date_series(s: pd.Series) -> pd.Series:
    return pd.to_datetime(s, errors="coerce", dayfirst=True).dt.date

def excel_days_to_time_from_series(s: pd.Series) -> pd.Series:
    td = pd.to_timedelta(pd.to_numeric(s, errors="coerce"), unit="D")
    return (pd.Timestamp("1899-12-30") + td).dt.time

def format_time_hms_series(tser: pd.Series) -> pd.Series:
    def fmt(x):
        if pd.isna(x): return None
        try: return x.strftime("%H:%M:%S")
        except Exception: return None
    return tser.apply(fmt)

def normalize_time(col: pd.Series) -> pd.Series:
    s = col.copy()
    if pd.api.types.is_timedelta64_dtype(s):
        t = (pd.Timestamp("1900-01-01") + s).dt.time
        return format_time_hms_series(t)
    if pd.api.types.is_numeric_dtype(s):
        t = excel_days_to_time_from_series(s)
        return format_time_hms_series(t)
    t = pd.to_datetime(s, errors="coerce").dt.time
    return format_time_hms_series(t)

FLOAT_COLS = [
    "HSAnimalCountTotal.0@1p","HSAnimalCountMilking.0","HSAnimalCountDry.0@1p","HSCountAnimElig.0",
    "HSInsemCountTotal@1p","HSInsemCountSuccess@1p","HSInsemRateCycle@1p","HSInsemSuccess@1p","HSPregRateCycle@1p",
    "HSAvgDIMFirstInsem@1p","HSAvgDaysOpen@1p","HSCountPreg@1p.0","HSAvgCalvingInterval",
    "HSAvgPeakMilk@1p","HSAvgPeakMilkDIM","HSAvgDailyMilk",
    "HSRollingHerdAvg@1p","HSRollingHerdAvg@1","HSRollingHerdAvg@2p",
    "HSAnimalCountLeft@1p"
]

def normalize_dataframe(df: pd.DataFrame, establo: str) -> pd.DataFrame:
    for c in COLUMNAS_SQL:
        if c not in df.columns:
            df[c] = np.nan
    df = df[COLUMNAS_SQL]

    # textos
    df["DairyName"] = df["DairyName"].fillna(establo)
    for c in ["Source.Name","DairyName"]:
        df[c] = df[c].astype(str).where(df[c].notna(), None)

    # fechas y hora
    for c in ["today()","HSDate","Source.FileDate"]:
        df[c] = to_date_series(df[c])
    df["Source.FileTime"] = normalize_time(df["Source.FileTime"])

    # decimales
    for c in FLOAT_COLS:
        df[c] = pd.to_numeric(df[c], errors="coerce")

    return df

def ts_str(dt: pd.Timestamp) -> str:
    return dt.strftime("%Y-%m-%d %H:%M:%S")

# ========= ÍNDICE SQL + FS =========
def cargar_indice_existente() -> dict:
    try:
        q = f"""
        SELECT [Source.Name] AS SName,
               CONVERT(varchar(10), [Source.FileDate], 120) + ' ' +
               CONVERT(varchar(8),  [Source.FileTime], 108) AS STS
        FROM dbo.{TABLE_NAME}
        WHERE [Source.Name] IS NOT NULL AND [Source.FileDate] IS NOT NULL AND [Source.FileTime] IS NOT NULL
        GROUP BY [Source.Name], [Source.FileDate], [Source.FileTime]
        """
        df = pd.read_sql_query(q, ENGINE)
        return (df.assign(dt=pd.to_datetime(df["STS"], errors="coerce"))
                  .dropna(subset=["dt"])
                  .sort_values(["SName","dt"])
                  .groupby("SName")["STS"].last()
                  .to_dict())
    except Exception as e:
        print(f"No se pudo leer índice existente: {e}")
        return {}

def escanear_archivos_fs() -> set:
    presentes = set()
    for establo in ESTABLOS:
        carpeta = os.path.join(BASE_PATH, establo, SUBFOLDER)
        if not os.path.isdir(carpeta):
            continue
        for f in os.listdir(carpeta):
            if f.lower().endswith(".xlsx") and not f.startswith("~$"):
                presentes.add(f)
    return presentes

# ========= BORRADO POR FALTANTES =========
def borrar_faltantes_en_fs():
    indice = cargar_indice_existente()
    en_sql = set(indice.keys())
    en_fs  = escanear_archivos_fs()
    faltantes = sorted(en_sql - en_fs)
    if not faltantes:
        print(f"✔ No hay archivos eliminados en FS pendientes de borrar en dbo.{TABLE_NAME}.")
        return
    print(f"🧹 Borrando {len(faltantes)} archivos inexistentes en FS…")
    with ENGINE.begin() as con:
        for nm in faltantes:
            con.execute(text(f"DELETE FROM dbo.{TABLE_NAME} WHERE [Source.Name] = :nm"), {"nm": nm})
            print(f"   - eliminado en SQL: {nm}")

# ========= CARGA INCREMENTAL =========
def cargar_establos_incremental():
    indice = cargar_indice_existente()

    for establo in ESTABLOS:
        carpeta = os.path.join(BASE_PATH, establo, SUBFOLDER)
        if not os.path.isdir(carpeta):
            continue

        archivos = [f for f in os.listdir(carpeta)
                    if f.lower().endswith(".xlsx") and not f.startswith("~$")]

        for archivo in archivos:
            ruta = os.path.join(carpeta, archivo)
            try:
                mtime = pd.to_datetime(os.path.getmtime(ruta), unit="s")
                actual_ts = ts_str(mtime)
                ts_prev = indice.get(archivo)

                if ts_prev is not None and pd.to_datetime(actual_ts) <= pd.to_datetime(ts_prev):
                    print(f"⏩ Saltando {archivo} (sin cambios, mtime {actual_ts})")
                    continue

                if ts_prev is not None and pd.to_datetime(actual_ts) > pd.to_datetime(ts_prev):
                    with ENGINE.begin() as con:
                        con.execute(text(f"DELETE FROM dbo.{TABLE_NAME} WHERE [Source.Name] = :nm"), {"nm": archivo})
                    print(f"🧹 Eliminado previo de {archivo} (TS previo {ts_prev})")

                # leer excel SIN encabezado, asignando nombres esperados
                df = pd.read_excel(ruta, header=None, names=COLUMNAS_EXCEL, engine="openpyxl")
                df.dropna(how="all", inplace=True)
                if df.empty:
                    print(f"⚠ {archivo} vacío, omitido.")
                    continue

                # metadatos
                df["Source.Name"]     = archivo
                df["Source.FileDate"] = mtime.date()
                df["Source.FileTime"] = mtime.strftime("%H:%M:%S")

                # normalizar tipos
                df = normalize_dataframe(df, establo)

                print(f"⏳ Cargando {archivo} desde {establo} ({actual_ts}) a dbo.{TABLE_NAME}…")
                df.to_sql(TABLE_NAME, ENGINE, if_exists="append", index=False, chunksize=1000, schema="dbo")
                print(f"✅ Cargado {archivo}.\n")

                indice[archivo] = actual_ts

            except Exception as e:
                print(f"❌ Error al cargar {archivo} desde {establo}: {e}\n")

if __name__ == "__main__":
    borrar_faltantes_en_fs()
    cargar_establos_incremental()

```

# 14) INSEMINACIONES

```python
import os
import urllib.parse
import pandas as pd
import numpy as np
from sqlalchemy import create_engine, text

# ========= CONFIG =========
SERVER   = "DESKTOP-3I6O45D"
DATABASE = "EVALUACION_ESTABLOS"
USER     = "sa"
PASSWORD = "123456789"

BASE_PATH = r"D:\PRUEBA PYTHON"
ESTABLOS = [
    "AGHOLSTEIN","ANGUS","ANTONIOJAIME","BALI","CANELAS","ESTRELLADEMANUEL",
    "GASUR","HUERTOS SAN MARTIN","LAREDO","MOCHERA","PIAMONTE","REMANSO",
    "SANTA FE","SAUSALITO","SEQUION"
]

TABLE_NAME = "SERVICIOS"
SUBFOLDER  = "SERVICIOS"   # ajusta si tu carpeta tiene otro nombre

# === ORDEN EXACTO DE COLUMNAS A INSERTAR (SIN IDENTITY) ===
COLUMNAS_SQL = ["Source.Name","DairyName","FECHA_DE_ARCHIVO","COW","LC_ACTUAL","DIM_ACTUAL","STAT_ACTUAL","RAZA","SEXO","SrvVAIdCode","SrvNumLact","SrvAmount","SrvCtrlCode","SrvCount3Day","SrvOffC","SrvCom2","SrvCount","SrvCntLf","SrvCost","SrvRevCode","SrvDim","SrvAge","SrvTech","SrvDat","SrvTime","SrvSidEffL2","SrvAct2hA","SrvDiag","SrvSire","FECHA_DE_PARTO",   "Source.FileDate","Source.FileTime"
]
# Columnas que vienen desde Excel (sin metadatos Source.*)
COLUMNAS_EXCEL = [c for c in COLUMNAS_SQL if not c.startswith("Source.")]

# ========= CONEXIÓN =========
def _make_engine(driver_name: str, encrypt_yes: bool):
    conn = (
        f"DRIVER={{{driver_name}}};SERVER={SERVER};DATABASE={DATABASE};UID={USER};PWD={PASSWORD};"
        f"TrustServerCertificate=Yes;Encrypt={'Yes' if encrypt_yes else 'No'};"
    )
    return create_engine(
        "mssql+pyodbc:///?odbc_connect=" + urllib.parse.quote_plus(conn),
        fast_executemany=True, pool_pre_ping=True
    )

def get_engine():
    for drv, enc in [("ODBC Driver 18 for SQL Server", True),
                     ("ODBC Driver 17 for SQL Server", False)]:
        try:
            eng = _make_engine(drv, enc)
            with eng.connect():
                pass
            print(f"Conectado con {drv}")
            return eng
        except Exception as e:
            print(f"No se pudo con {drv}: {e}")
    raise RuntimeError("No se pudo abrir conexión ODBC 17/18.")

ENGINE = get_engine()

# ========= NORMALIZACIONES =========
def to_date_series(s: pd.Series) -> pd.Series:
    return pd.to_datetime(s, errors="coerce", dayfirst=True).dt.date

def excel_days_to_time_from_series(s: pd.Series) -> pd.Series:
    td = pd.to_timedelta(pd.to_numeric(s, errors="coerce"), unit="D")
    return (pd.Timestamp("1899-12-30") + td).dt.time

def format_time_hms_series(tser: pd.Series) -> pd.Series:
    def fmt(x):
        if pd.isna(x):
            return None
        try:
            return x.strftime("%H:%M:%S")
        except Exception:
            return None
    return tser.apply(fmt)

def normalize_time(col: pd.Series) -> pd.Series:
    s = col.copy()
    # Excel serial o timedelta
    if pd.api.types.is_timedelta64_dtype(s):
        t = (pd.Timestamp("1900-01-01") + s).dt.time
        return format_time_hms_series(t)
    if pd.api.types.is_numeric_dtype(s):
        t = excel_days_to_time_from_series(s)
        return format_time_hms_series(t)
    # Texto u objeto datetime
    t = pd.to_datetime(s, errors="coerce").dt.time
    return format_time_hms_series(t)

def to_decimal_nullable(series: pd.Series) -> pd.Series:
    return pd.to_numeric(series, errors="coerce")

def cow_to_str(v):
    if pd.isna(v): return None
    if isinstance(v, (int, np.integer)): return str(int(v))
    if isinstance(v, float) and v.is_integer(): return str(int(v))
    return str(v)

def normalize_dataframe(df: pd.DataFrame, establo: str) -> pd.DataFrame:
    # Asegurar columnas y orden
    for c in COLUMNAS_SQL:
        if c not in df.columns:
            df[c] = np.nan
    df = df[COLUMNAS_SQL]

    # Defaults
    df["DairyName"] = df["DairyName"].fillna(establo)

    # Fechas
    for c in ["FECHA_DE_ARCHIVO","SrvDat","FECHA_DE_PARTO","Source.FileDate"]:
        df[c] = to_date_series(df[c])

    # Horas
    for c in ["SrvTime", "Source.FileTime"]:
        df[c] = normalize_time(df[c])

    # Texto
    text_cols = [
        "Source.Name","DairyName","COW","STAT ACTUAL","RAZA","SEXO","SrvOffC","SrvCom2","SrvTech","SrvDiag","SrvSire"

    ]
    for c in text_cols:
        df[c] = df[c].astype(str).where(df[c].notna(), None)

    # cow como string sin .0
    df["COW"] = df["COW"].apply(cow_to_str)

    # Numéricos enteros
    int_cols = [
        "LC_ACTUAL","DIM_ACTUAL","PrgNumLact","PrgCount","PrgCntLf","PrgDim","Prg2Do"
    ]
    for c in int_cols:
        df[c] = pd.to_numeric(df[c], errors="coerce").astype("Int64")

    # Numéricos decimales
    for c in ["PrgCost","PrgAge"]:
        df[c] = to_decimal_nullable(df[c])

    # Timedelta defensivo
    for c in df.columns:
        if pd.api.types.is_timedelta64_dtype(df[c]):
            df[c] = format_time_hms_series((pd.Timestamp("1900-01-01") + df[c]).dt.time)

    return df

def ts_str(dt: pd.Timestamp) -> str:
    return dt.strftime("%Y-%m-%d %H:%M:%S")

# ========= ÍNDICE SQL + FS =========
def cargar_indice_existente() -> dict:
    try:
        q = f"""
        SELECT [Source.Name] AS SName,
               CONVERT(varchar(10), [Source.FileDate], 120) + ' ' +
               CONVERT(varchar(8),  [Source.FileTime], 108) AS STS
        FROM dbo.{TABLE_NAME}
        WHERE [Source.Name] IS NOT NULL AND [Source.FileDate] IS NOT NULL AND [Source.FileTime] IS NOT NULL
        GROUP BY [Source.Name], [Source.FileDate], [Source.FileTime]
        """
        df = pd.read_sql_query(q, ENGINE)
        return (df.assign(dt=pd.to_datetime(df["STS"], errors="coerce"))
                  .dropna(subset=["dt"])
                  .sort_values(["SName","dt"])
                  .groupby("SName")["STS"].last()
                  .to_dict())
    except Exception as e:
        print(f"No se pudo leer índice existente: {e}")
        return {}

def escanear_archivos_fs() -> set:
    presentes = set()
    for establo in ESTABLOS:
        carpeta = os.path.join(BASE_PATH, establo, SUBFOLDER)
        if not os.path.isdir(carpeta):
            continue
        for f in os.listdir(carpeta):
            if f.lower().endswith(".xlsx") and not f.startswith("~$"):
                presentes.add(f)
    return presentes

# ========= BORRADO POR FALTANTES =========
def borrar_faltantes_en_fs():
    indice = cargar_indice_existente()
    en_sql = set(indice.keys())
    en_fs  = escanear_archivos_fs()
    faltantes = sorted(en_sql - en_fs)
    if not faltantes:
        print(f"✔ No hay archivos eliminados en FS pendientes de borrar en dbo.{TABLE_NAME}.")
        return
    print(f"🧹 Borrando {len(faltantes)} archivos inexistentes en FS…")
    with ENGINE.begin() as con:
        for nm in faltantes:
            con.execute(text(f"DELETE FROM dbo.{TABLE_NAME} WHERE [Source.Name] = :nm"), {"nm": nm})
            print(f"   - eliminado en SQL: {nm}")

# ========= CARGA INCREMENTAL =========
def cargar_establos_incremental():
    indice = cargar_indice_existente()

    for establo in ESTABLOS:
        carpeta = os.path.join(BASE_PATH, establo, SUBFOLDER)
        if not os.path.isdir(carpeta):
            continue

        archivos = [f for f in os.listdir(carpeta)
                    if f.lower().endswith(".xlsx") and not f.startswith("~$")]

        for archivo in archivos:
            ruta = os.path.join(carpeta, archivo)
            try:
                mtime = pd.to_datetime(os.path.getmtime(ruta), unit="s")
                actual_ts = ts_str(mtime)
                ts_prev = indice.get(archivo)

                if ts_prev is not None and pd.to_datetime(actual_ts) <= pd.to_datetime(ts_prev):
                    print(f"⏩ Saltando {archivo} (sin cambios, mtime {actual_ts})")
                    continue

                if ts_prev is not None and pd.to_datetime(actual_ts) > pd.to_datetime(ts_prev):
                    with ENGINE.begin() as con:
                        con.execute(text(f"DELETE FROM dbo.{TABLE_NAME} WHERE [Source.Name] = :nm"), {"nm": archivo})
                    print(f"🧹 Eliminado previo de {archivo} (TS previo {ts_prev})")

                # Leer excel SIN encabezado con nombres esperados
                df = pd.read_excel(ruta, header=None, names=COLUMNAS_EXCEL, engine="openpyxl")
                df.dropna(how="all", inplace=True)
                if df.empty:
                    print(f"⚠ {archivo} vacío, omitido.")
                    continue

                # Metadatos
                df["Source.Name"]     = archivo
                df["Source.FileDate"] = mtime.date()
                df["Source.FileTime"] = mtime.strftime("%H:%M:%S")

                # Normalizar tipos
                df = normalize_dataframe(df, establo)

                print(f"⏳ Cargando {archivo} desde {establo} ({actual_ts}) a dbo.{TABLE_NAME}…")
                df.to_sql(TABLE_NAME, ENGINE, if_exists="append", index=False, chunksize=1000, schema="dbo")
                print(f"✅ Cargado {archivo}.\n")

                indice[archivo] = actual_ts

            except Exception as e:
                print(f"❌ Error al cargar {archivo} desde {establo}: {e}\n")

if __name__ == "__main__":
    borrar_faltantes_en_fs()
    cargar_establos_incremental()

```