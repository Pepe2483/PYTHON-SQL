#ABORTOS

import os
import hashlib
import urllib.parse
from datetime import datetime
import pandas as pd
from sqlalchemy import create_engine, text

# ========= CONFIG =========
SERVER   = r"localhost\SQLEXPRESS"
DATABASE = "EVALUACION_ESTABLOS"
USER     = "sa"
PASSWORD = "123456789"

BASE_PATH = r"C:\Users\JOSE\Dropbox\JOSE-DP-SHEILA\EVALUACION DE ESTABLO POR TABLAS\TODOS_LOS_ESTABLOS_CONSUBCARPETAS"
EVENTO = "PARTOS"
SUBCARPETA_EVENTO = "PARTOS"

RAW_SCHEMA = "raw"
RAW_TABLE  = "PARTOS"

COLUMNAS_SQL = [
    "Source_Name","DairyName","TODAY","cow","lc","dim","stat","bd","typ",
    "ClvVAIdCode","ClvNumLact","ClvCtrlCode","ClvOffC","ClvCom","ClvCom2",
    "ClvCount","ClvCntLf","ClvCost","ClvRevCode","ClvDim","ClvAge","ClvTech",
    "ClvClvEase","ClvClvEaseCod","ClvDat","ClvTime","Clv2Do","ClvSidEffL2",
    "ClvDiag","Source_FileDate","Source_FileTime"
]

# ========= CONEXIÓN =========
def _engine(driver_name: str):
    conn = (
        f"DRIVER={{{driver_name}}};SERVER={SERVER};DATABASE={DATABASE};"
        f"UID={USER};PWD={PASSWORD};TrustServerCertificate=Yes;Encrypt=No;"
    )
    return create_engine(
        "mssql+pyodbc:///?odbc_connect=" + urllib.parse.quote_plus(conn),
        fast_executemany=True, pool_pre_ping=True
    )

def get_engine():
    for drv in ["ODBC Driver 18 for SQL Server", "ODBC Driver 17 for SQL Server"]:
        try:
            eng = _engine(drv)
            with eng.connect():
                pass
            print(f"✅ Conectado con {drv}")
            return eng
        except Exception as e:
            print(f"❌ No se pudo con {drv}: {e}")
    raise RuntimeError("No se pudo conectar con ODBC 17/18.")

ENGINE = get_engine()

# ========= UTIL =========
def sha256_file(path: str) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()

def listar_establos() -> list[str]:
    return sorted([d for d in os.listdir(BASE_PATH) if os.path.isdir(os.path.join(BASE_PATH, d))])

def listar_archivos():
    items = []
    for establo in listar_establos():
        carpeta = os.path.join(BASE_PATH, establo, SUBCARPETA_EVENTO)
        if not os.path.isdir(carpeta):
            continue
        for f in os.listdir(carpeta):
            fl = f.lower()
            if (fl.endswith(".xlsx") or fl.endswith(".xlsm")) and not f.startswith("~$"):
                path = os.path.join(carpeta, f)
                items.append((establo, f, path))
    return items

def ensure_raw_table():
    cols = ",\n".join([f"[{c}] nvarchar(2000) NULL" for c in COLUMNAS_SQL])
    ddl = f"""
    IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name='{RAW_SCHEMA}')
      EXEC('CREATE SCHEMA {RAW_SCHEMA}');
    IF OBJECT_ID('{RAW_SCHEMA}.{RAW_TABLE}','U') IS NULL
    BEGIN
      CREATE TABLE {RAW_SCHEMA}.{RAW_TABLE}(
        RawId bigint IDENTITY(1,1) NOT NULL PRIMARY KEY,
        {cols}
      );
    END
    """
    with ENGINE.begin() as con:
        con.execute(text(ddl))

def get_registry():
    q = """
    SELECT DairyName, SourceName, FileHashSha256, ISNULL(MissingCount,0) AS MissingCount
    FROM etl.FileRegistry
    WHERE EventName = :ev
    """
    return pd.read_sql_query(text(q), ENGINE, params={"ev": EVENTO})

def upsert_registry(dn, sn, path, sizeb, sha, mtime, rows_loaded, missing_count=0):
    sql = """
    MERGE etl.FileRegistry AS T
    USING (SELECT :ev AS EventName, :dn AS DairyName, :sn AS SourceName) AS S
      ON (T.EventName=S.EventName AND T.DairyName=S.DairyName AND T.SourceName=S.SourceName)
    WHEN MATCHED THEN
      UPDATE SET SourcePath=:sp, FileSizeBytes=:sz, FileHashSha256=:hs, SourceMTime=:mt,
                 RowsLoaded=:rl, MissingCount=:mc, LastSeenAt=SYSUTCDATETIME(), LoadedAt=SYSUTCDATETIME()
    WHEN NOT MATCHED THEN
      INSERT (EventName, DairyName, SourceName, SourcePath, FileSizeBytes, FileHashSha256, SourceMTime, RowsLoaded, MissingCount, LastSeenAt)
      VALUES (:ev, :dn, :sn, :sp, :sz, :hs, :mt, :rl, :mc, SYSUTCDATETIME());
    """
    with ENGINE.begin() as con:
        con.execute(text(sql), {"ev": EVENTO, "dn": dn, "sn": sn, "sp": path, "sz": sizeb,
                                "hs": sha, "mt": mtime, "rl": rows_loaded, "mc": missing_count})

def mark_missing(dn, sn, new_mc):
    sql = """
    UPDATE etl.FileRegistry
    SET MissingCount=:mc, LoadedAt=SYSUTCDATETIME()
    WHERE EventName=:ev AND DairyName=:dn AND SourceName=:sn
    """
    with ENGINE.begin() as con:
        con.execute(text(sql), {"ev": EVENTO, "dn": dn, "sn": sn, "mc": new_mc})

def delete_file_rows(dn, sn):
    with ENGINE.begin() as con:
        con.execute(text(f"DELETE FROM {RAW_SCHEMA}.{RAW_TABLE} WHERE [DairyName]=:dn AND [Source_Name]=:sn"),
                    {"dn": dn, "sn": sn})

def purge_file(dn, sn):
    with ENGINE.begin() as con:
        con.execute(text(f"DELETE FROM {RAW_SCHEMA}.{RAW_TABLE} WHERE [DairyName]=:dn AND [Source_Name]=:sn"),
                    {"dn": dn, "sn": sn})
        con.execute(text("DELETE FROM etl.FileRegistry WHERE EventName=:ev AND DairyName=:dn AND SourceName=:sn"),
                    {"ev": EVENTO, "dn": dn, "sn": sn})

def read_excel_raw(path: str) -> pd.DataFrame:
    columnas_excel = [c for c in COLUMNAS_SQL if c not in {"Source_Name","Source_FileDate","Source_FileTime"}]
    df = pd.read_excel(path, header=None, names=columnas_excel, engine="openpyxl")
    df.dropna(how="all", inplace=True)
    if df.empty:
        return df
    for c in df.columns:
        df[c] = df[c].apply(lambda x: None if pd.isna(x) else str(x))
    return df

def main():
    ensure_raw_table()

    reg = get_registry()
    reg_map = {(r.DairyName, r.SourceName): (r.FileHashSha256, int(r.MissingCount)) for r in reg.itertuples()} if not reg.empty else {}

    fs_items = listar_archivos()
    fs_set = {(dn, sn) for dn, sn, _ in fs_items}

    # faltantes: recién purga si falta 2 corridas seguidas
    for (dn, sn), (_, old_mc) in reg_map.items():
        if (dn, sn) not in fs_set:
            new_mc = old_mc + 1
            if new_mc >= 2:
                print(f"🧹 PURGA (faltó 2 corridas): {dn} | {sn}")
                purge_file(dn, sn)
            else:
                print(f"⚠ Missing 1/2: {dn} | {sn}")
                mark_missing(dn, sn, new_mc)

    # presentes: hash => skip/recarga/nuevo
    for dn, sn, path in fs_items:
        sizeb = os.path.getsize(path)
        mtime = datetime.fromtimestamp(os.path.getmtime(path))
        sha = sha256_file(path)

        prev = reg_map.get((dn, sn))
        prev_hash = prev[0] if prev else None

        if prev_hash == sha:
            upsert_registry(dn, sn, path, sizeb, sha, mtime, rows_loaded=None, missing_count=0)
            print(f"⏩ SKIP: {dn} | {sn}")
            continue

        if prev_hash is not None and prev_hash != sha:
            print(f"🔁 RECARGA: {dn} | {sn}")
            delete_file_rows(dn, sn)
        else:
            print(f"✅ NUEVO: {dn} | {sn}")

        df = read_excel_raw(path)
        if df.empty:
            upsert_registry(dn, sn, path, sizeb, sha, mtime, rows_loaded=0, missing_count=0)
            print(f"⚠ vacío: {dn} | {sn}")
            continue

        df["Source_Name"] = sn
        df["DairyName"] = dn
        df["Source_FileDate"] = mtime.strftime("%Y-%m-%d")
        df["Source_FileTime"] = mtime.strftime("%H:%M:%S.%f")[:-3]

        for c in COLUMNAS_SQL:
            if c not in df.columns:
                df[c] = None
        df = df[COLUMNAS_SQL]

        df.to_sql(RAW_TABLE, ENGINE, schema=RAW_SCHEMA, if_exists="append", index=False, chunksize=2000)
        upsert_registry(dn, sn, path, sizeb, sha, mtime, rows_loaded=len(df), missing_count=0)
        print(f"✅ CARGADO: {dn} | {sn} ({len(df)} filas)\n")

if __name__ == "__main__":
    main()
