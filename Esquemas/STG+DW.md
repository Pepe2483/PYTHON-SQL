USE EVALUACION_ESTABLOS;
GO

/* 0) Schemas (por si faltan) */
IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'stg') EXEC('CREATE SCHEMA stg AUTHORIZATION dbo');
IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'dw')  EXEC('CREATE SCHEMA dw  AUTHORIZATION dbo');
GO

/* 1) Lista de tablas dbo a “stg” + “dw view” */
DECLARE @Tablas TABLE (Tabla SYSNAME);
INSERT INTO @Tablas (Tabla)
VALUES
('ABORTOS'),
('ENFERMEDADES'),
('GENERALES_ESTABLOS'),
('GENERALES_RECRIA'),
('GENERALES_VACAS'),
('HATO_ACTUAL'),
('LACTACIONES'),
('NACIMIENTOS'),
('PARTOS'),
('PROGENIE'),
('SACAS'),
('SECADO'),
('SERVICIOS'),
('TEST_DE_PRENEZ');

DECLARE @tbl SYSNAME;
DECLARE @sql NVARCHAR(MAX);
DECLARE @cols NVARCHAR(MAX);
DECLARE @hasRowNum BIT;
DECLARE @hasLoadDts BIT;
DECLARE @hasSourceName BIT;

DECLARE c CURSOR FAST_FORWARD FOR
SELECT Tabla FROM @Tablas;

OPEN c;
FETCH NEXT FROM c INTO @tbl;

WHILE @@FETCH_STATUS = 0
BEGIN
    /* Flags de columnas especiales (por seguridad) */
    SELECT
        @hasRowNum    = CASE WHEN EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID('dbo.' + @tbl) AND name = 'RowNum') THEN 1 ELSE 0 END,
        @hasLoadDts   = CASE WHEN EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID('dbo.' + @tbl) AND name = 'LoadDts') THEN 1 ELSE 0 END,
        @hasSourceName= CASE WHEN EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID('dbo.' + @tbl) AND name = 'Source_Name') THEN 1 ELSE 0 END;

    /* 2) Crear stg.<tabla> (si no existe) */
    IF OBJECT_ID('stg.' + @tbl, 'U') IS NULL
    BEGIN
        /* Columnas: copiamos estructura de dbo.<tabla> (sin identity ni computed) */
        SELECT @cols =
            STRING_AGG(
                '    ' + QUOTENAME(c.name) + ' ' +
                CASE
                    WHEN ty.name IN ('varchar','char','varbinary','binary')
                        THEN ty.name + '(' + CASE WHEN c.max_length = -1 THEN 'max' ELSE CAST(c.max_length AS varchar(10)) END + ')'
                    WHEN ty.name IN ('nvarchar','nchar')
                        THEN ty.name + '(' + CASE WHEN c.max_length = -1 THEN 'max' ELSE CAST(c.max_length/2 AS varchar(10)) END + ')'
                    WHEN ty.name IN ('decimal','numeric')
                        THEN ty.name + '(' + CAST(c.precision AS varchar(10)) + ',' + CAST(c.scale AS varchar(10)) + ')'
                    WHEN ty.name IN ('datetime2','time','datetimeoffset')
                        THEN ty.name + '(' + CAST(c.scale AS varchar(10)) + ')'
                    ELSE ty.name
                END +
                CASE WHEN c.is_nullable = 1 THEN ' NULL' ELSE ' NOT NULL' END
            , ',' + CHAR(10)
            ) WITHIN GROUP (ORDER BY c.column_id)
        FROM sys.columns c
        JOIN sys.types ty ON ty.user_type_id = c.user_type_id
        WHERE c.object_id = OBJECT_ID('dbo.' + @tbl)
          AND c.is_identity = 0
          AND c.is_computed = 0;

        /* Armado CREATE TABLE */
        SET @sql = N'CREATE TABLE stg.' + QUOTENAME(@tbl) + N' (' + CHAR(10) +
                   N'    StgId BIGINT IDENTITY(1,1) NOT NULL,' + CHAR(10) +
                   CASE WHEN @hasLoadDts = 1 THEN N'' ELSE N'    LoadDts DATETIME2(0) NOT NULL CONSTRAINT DF_stg_' + REPLACE(@tbl,' ','_') + N'_LoadDts DEFAULT SYSUTCDATETIME(),' + CHAR(10) END +
                   CASE WHEN @hasRowNum  = 1 THEN N'' ELSE N'    RowNum  INT NULL,' + CHAR(10) END +
                   @cols + CHAR(10) +
                   N'    ,CONSTRAINT PK_stg_' + REPLACE(@tbl,' ','_') + N' PRIMARY KEY CLUSTERED (StgId)' + CHAR(10) +
                   N');';

        EXEC sp_executesql @sql;
    END

    /* 3) Índice recomendado en stg: (Source_Name, RowNum) si existe Source_Name */
    IF @hasSourceName = 1 AND COL_LENGTH('stg.' + @tbl, 'RowNum') IS NOT NULL
    BEGIN
        DECLARE @ix SYSNAME = N'IX_stg_' + REPLACE(@tbl,' ','_') + N'_Source_Row';

        IF EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID('stg.' + @tbl) AND name = @ix)
        BEGIN
            SET @sql = N'DROP INDEX ' + QUOTENAME(@ix) + N' ON stg.' + QUOTENAME(@tbl) + N';';
            EXEC sp_executesql @sql;
        END

        SET @sql = N'CREATE INDEX ' + QUOTENAME(@ix) + N' ON stg.' + QUOTENAME(@tbl) + N' (Source_Name, RowNum);';
        EXEC sp_executesql @sql;
    END

    /* 4) Crear vista dw.vw_<tabla> apuntando a stg.<tabla> */
    SET @sql = N'CREATE OR ALTER VIEW dw.vw_' + REPLACE(@tbl,' ','_') + N' AS
                 SELECT * FROM stg.' + QUOTENAME(@tbl) + N';';
    EXEC sp_executesql @sql;

    FETCH NEXT FROM c INTO @tbl;
END

CLOSE c;
DEALLOCATE c;
GO

/* 5) Verificación: qué se creó */
SELECT s.name AS schema_name, t.name AS object_name, 'TABLE' AS object_type
FROM sys.tables t
JOIN sys.schemas s ON s.schema_id = t.schema_id
WHERE s.name IN ('stg')
UNION ALL
SELECT s.name, v.name, 'VIEW'
FROM sys.views v
JOIN sys.schemas s ON s.schema_id = v.schema_id
WHERE s.name IN ('dw')
ORDER BY schema_name, object_type, object_name;
GO
