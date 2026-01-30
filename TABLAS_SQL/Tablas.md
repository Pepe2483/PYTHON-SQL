# Crear tablas en Sql sin espacion ni putnos en los nombre
# PARTOS

USE EVALUACION_ESTABLOS;
GO


IF OBJECT_ID('dbo.PARTOS','U') IS NULL
BEGIN
  CREATE TABLE dbo.PARTOS (
    OID_PARTO          INT IDENTITY(1,1) PRIMARY KEY,
    [Source.Name]      VARCHAR(80) NULL,
    DairyName          VARCHAR(50) NULL,
    TODAY              DATE        NULL,
    cow                BIGINT      NULL,
    lc                 INT         NULL,
    dim                INT         NULL,
    stat               VARCHAR(20) NULL,
    bd                 DATE        NULL,
    typ                VARCHAR(20) NULL,
    ClvVAIdCode        VARCHAR(20) NULL,
    ClvNumLact         INT         NULL,
    ClvCtrlCode        VARCHAR(20) NULL,
    ClvOffC            INT         NULL,
    ClvCom             VARCHAR(50) NULL,
    ClvCom2            VARCHAR(50) NULL,
    ClvCount           INT         NULL,
    ClvCntLf           INT         NULL,
    ClvCost            FLOAT       NULL,
    ClvRevCode         VARCHAR(20) NULL,
    ClvDim             INT         NULL,
    ClvAge             INT         NULL,
    ClvTech            VARCHAR(20) NULL,
    ClvDat             DATE        NULL,
    ClvTime            TIME(0)     NULL,
    Clv2Do             INT         NULL,
    ClvSidEffL2        VARCHAR(20) NULL,
    ClvDiag            VARCHAR(50) NULL,
    Source_FileDate    DATE        NULL,
    Source_FileTime    TIME(0)     NULL
  );
END
GO
# ABORTOS
