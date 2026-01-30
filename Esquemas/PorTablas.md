#ABORTOS

USE EVALUACION_ESTABLOS;
GO

-- 1) Crear schema raw si no existe
IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'raw')
    EXEC('CREATE SCHEMA raw AUTHORIZATION dbo');
GO

-- 2) Crear tabla raw.ABORTOS si no existe (clonando columnas de dbo.ABORTOS como NVARCHAR)
IF OBJECT_ID('raw.ABORTOS', 'U') IS NULL
BEGIN
    DECLARE @sql NVARCHAR(MAX);

    SELECT @sql =
        N'CREATE TABLE raw.ABORTOS (' + CHAR(10) +
        N'    RawId   BIGINT IDENTITY(1,1) NOT NULL,' + CHAR(10) +
        N'    LoadDts DATETIME2(0) NOT NULL CONSTRAINT DF_raw_ABORTOS_LoadDts DEFAULT SYSUTCDATETIME(),' + CHAR(10) +
        N'    RowNum  INT NULL,' + CHAR(10) +
        STRING_AGG(
            N'    ' + QUOTENAME(c.name) + N' NVARCHAR(4000) NULL'
        , ',' + CHAR(10)) WITHIN GROUP (ORDER BY c.column_id) + CHAR(10) +
        N'    ,CONSTRAINT PK_raw_ABORTOS PRIMARY KEY CLUSTERED (RawId)' + CHAR(10) +
        N');'
    FROM sys.columns c
    JOIN sys.objects o ON o.object_id = c.object_id
    JOIN sys.schemas s ON s.schema_id = o.schema_id
    WHERE s.name = 'dbo'
      AND o.name = 'ABORTOS'
      AND c.name NOT IN ('RawId','LoadDts','RowNum');

    EXEC sp_executesql @sql;

    -- 3) Índice útil para tu incremental (archivo + fila)
    IF COL_LENGTH('raw.ABORTOS','Source_Name') IS NOT NULL
       AND COL_LENGTH('raw.ABORTOS','RowNum') IS NOT NULL
    BEGIN
        CREATE INDEX IX_raw_ABORTOS_Source_Row
        ON raw.ABORTOS (Source_Name, RowNum);
    END
END



USE EVALUACION_ESTABLOS;

-- 1) Borrar el índice actual
DROP INDEX IF EXISTS IX_raw_ABORTOS_Source_Row ON raw.ABORTOS;

-- 2) Reducir tamaños de metadatos (solo esto)
ALTER TABLE raw.ABORTOS ALTER COLUMN Source_Name NVARCHAR(260) NULL;
ALTER TABLE raw.ABORTOS ALTER COLUMN DairyName   NVARCHAR(60)  NULL;

-- 3) Recrear índice seguro
CREATE INDEX IX_raw_ABORTOS_Source_Row
ON raw.ABORTOS (Source_Name, RowNum);
