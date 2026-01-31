
#CREAR Todos RAW

USE EVALUACION_ESTABLOS;
GO

/* 0) Asegurar schema raw */
IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'raw')
    EXEC('CREATE SCHEMA raw AUTHORIZATION dbo');
GO

/* 1) Lista de tablas dbo a clonar a raw */
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

DECLARE @tbl SYSNAME, @sql NVARCHAR(MAX), @ix SYSNAME;

DECLARE c CURSOR FAST_FORWARD FOR
SELECT Tabla FROM @Tablas;

OPEN c;
FETCH NEXT FROM c INTO @tbl;

WHILE @@FETCH_STATUS = 0
BEGIN
    /* 2) Crear raw.<tabla> si no existe */
    IF OBJECT_ID(N'raw.' + @tbl, 'U') IS NULL
    BEGIN
        SELECT @sql =
            N'CREATE TABLE raw.' + QUOTENAME(@tbl) + N' (' + CHAR(10) +
            N'    RawId   BIGINT IDENTITY(1,1) NOT NULL,' + CHAR(10) +
            N'    LoadDts DATETIME2(0) NOT NULL CONSTRAINT DF_raw_' + REPLACE(@tbl,' ','_') + N'_LoadDts DEFAULT SYSUTCDATETIME(),' + CHAR(10) +
            N'    RowNum  INT NULL,' + CHAR(10) +
            STRING_AGG(
                N'    ' + QUOTENAME(c2.name) + N' ' +
                CASE 
                    WHEN c2.name = 'Source_Name'     THEN 'NVARCHAR(260) NULL'
                    WHEN c2.name = 'DairyName'       THEN 'NVARCHAR(60) NULL'
                    WHEN c2.name = 'Source_FileTime' THEN 'NVARCHAR(8) NULL'
                    ELSE 'NVARCHAR(4000) NULL'
                END
            , ',' + CHAR(10)) WITHIN GROUP (ORDER BY c2.column_id) + CHAR(10) +
            N'   ,CONSTRAINT PK_raw_' + REPLACE(@tbl,' ','_') + N' PRIMARY KEY CLUSTERED (RawId)' + CHAR(10) +
            N');'
        FROM sys.columns c2
        JOIN sys.objects o2 ON o2.object_id = c2.object_id
        JOIN sys.schemas s2 ON s2.schema_id = o2.schema_id
        WHERE s2.name = 'dbo'
          AND o2.name = @tbl
          AND c2.name NOT IN ('RawId','LoadDts','RowNum');

        EXEC sp_executesql @sql;
    END

    /* 3) Ajustar tamaños (por si existían con NVARCHAR(4000)) */
    IF COL_LENGTH('raw.' + @tbl, 'Source_Name') IS NOT NULL
    BEGIN
        SET @sql = N'ALTER TABLE raw.' + QUOTENAME(@tbl) + N' ALTER COLUMN Source_Name NVARCHAR(260) NULL;';
        EXEC sp_executesql @sql;
    END

    IF COL_LENGTH('raw.' + @tbl, 'DairyName') IS NOT NULL
    BEGIN
        SET @sql = N'ALTER TABLE raw.' + QUOTENAME(@tbl) + N' ALTER COLUMN DairyName NVARCHAR(60) NULL;';
        EXEC sp_executesql @sql;
    END

    IF COL_LENGTH('raw.' + @tbl, 'Source_FileTime') IS NOT NULL
    BEGIN
        SET @sql = N'ALTER TABLE raw.' + QUOTENAME(@tbl) + N' ALTER COLUMN Source_FileTime NVARCHAR(8) NULL;';
        EXEC sp_executesql @sql;
    END

    /* 4) Índice (Source_Name, RowNum) si existe Source_Name */
    IF COL_LENGTH('raw.' + @tbl, 'Source_Name') IS NOT NULL
    BEGIN
        SET @ix = N'IX_raw_' + REPLACE(@tbl,' ','_') + N'_Source_Row';

        IF EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID('raw.' + @tbl, 'U') AND name = @ix)
        BEGIN
            SET @sql = N'DROP INDEX ' + QUOTENAME(@ix) + N' ON raw.' + QUOTENAME(@tbl) + N';';
            EXEC sp_executesql @sql;
        END

        SET @sql = N'CREATE INDEX ' + QUOTENAME(@ix) + N' ON raw.' + QUOTENAME(@tbl) + N' (Source_Name, RowNum);';
        EXEC sp_executesql @sql;
    END

    FETCH NEXT FROM c INTO @tbl;
END

CLOSE c;
DEALLOCATE c;
GO

/* 5) Verificación final */
SELECT s.name AS schema_name, t.name AS table_name
FROM sys.tables t
JOIN sys.schemas s ON s.schema_id = t.schema_id
WHERE s.name = 'raw'
ORDER BY t.name;
GO


#Comprobar si se crearon las tablas
USE EVALUACION_ESTABLOS;

-- 1) Borrar el índice actual
DROP INDEX IF EXISTS IX_raw_ABORTOS_Source_Row ON raw.ABORTOS;

-- 2) Reducir tamaños de metadatos (solo esto)
ALTER TABLE raw.ABORTOS ALTER COLUMN Source_Name NVARCHAR(260) NULL;
ALTER TABLE raw.ABORTOS ALTER COLUMN DairyName   NVARCHAR(60)  NULL;

-- 3) Recrear índice seguro
CREATE INDEX IX_raw_ABORTOS_Source_Row
ON raw.ABORTOS (Source_Name, RowNum);
