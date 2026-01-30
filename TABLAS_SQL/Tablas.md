# Crear tablas en Sql sin espacion ni putnos en los nombre
# PARTOS

USE EVALUACION_ESTABLOS;
GO


IF OBJECT_ID('dbo.PARTOS','U') IS NULL
BEGIN
  CREATE TABLE dbo.PARTOS (
    OID_PARTO          INT IDENTITY(1,1) PRIMARY KEY,
    [Source_Name]      VARCHAR(80) NULL,
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
IF OBJECT_ID('dbo.ABORTOS','U') IS NULL
BEGIN
  CREATE TABLE dbo.ABORTOS (
    OID_ABORTO         INT IDENTITY(1,1) PRIMARY KEY,
    [Source_Name]      VARCHAR(80) NULL,
    DairyName          VARCHAR(50) NULL,
    TODAY              DATE        NULL,
    COW                BIGINT      NULL,
    STAT               VARCHAR(20) NULL,
    BD                 DATE        NULL,
    TYP                VARCHAR(20) NULL,
    AbtVAIdCode        VARCHAR(20) NULL,
    AbtNumLact         INT         NULL,
    AbtCtrlCode        VARCHAR(20) NULL,
    AbtOffC            INT         NULL,
    AbtCom             VARCHAR(50) NULL,
    AbtCom2            VARCHAR(50) NULL,
    AbtCount           INT         NULL,
    AbtCntLf           INT         NULL,
    AbtCost            FLOAT       NULL,
    AbtDim             INT         NULL,
    AbtAge             INT         NULL,
    AbtTech            VARCHAR(20) NULL,
    AbtClvEase         VARCHAR(20) NULL,
    AbtClvEaseCod      VARCHAR(20) NULL,
    AbtDat             DATE        NULL,
    AbtTime            TIME(0)     NULL,
    AbtSidEffL2        VARCHAR(20) NULL,
    AbtDiag            VARCHAR(50) NULL,
    FECHA_DE_PARTO     DATE        NULL,
    Source_FileDate    DATE        NULL,
    Source_FileTime    TIME(0)     NULL
  );
END
GO

# SECADO
IF OBJECT_ID('dbo.SECADO','U') IS NULL
BEGIN
  CREATE TABLE dbo.SECADO (
    OID_SECA           INT IDENTITY(1,1) PRIMARY KEY,
    [Source_Name]      VARCHAR(80) NULL,
    DairyName          VARCHAR(50) NULL,
    TODAY              DATE        NULL,
    cow                BIGINT      NULL,
    stat               VARCHAR(20) NULL,
    bd                 DATE        NULL,
    typ                VARCHAR(20) NULL,
    DryVAIdCode        VARCHAR(20) NULL,
    DryNumLact         INT         NULL,
    DryCtrlCode        VARCHAR(20) NULL,
    DryOffC            INT         NULL,
    DryCom             VARCHAR(50) NULL,
    DryCom2            VARCHAR(50) NULL,
    DryCount           INT         NULL,
    DryCntLf           INT         NULL,
    DryCost            FLOAT       NULL,
    DryRevCode         VARCHAR(20) NULL,
    DryDim             INT         NULL,
    DryAge             INT         NULL,
    DryTech            VARCHAR(20) NULL,
    DryDat             DATE        NULL,
    DryTime            TIME(0)     NULL,
    DrySidEffL2        VARCHAR(20) NULL,
    DryDiag            VARCHAR(50) NULL,
    FECHAPARTO         DATE        NULL,
    Source_FileDate    DATE        NULL,
    Source_FileTime    TIME(0)     NULL
  );
END
GO

#SECADO

USE EVALUACION_ESTABLOS;
GO

IF OBJECT_ID('dbo.SECADO','U') IS NULL
BEGIN
  CREATE TABLE dbo.SECADO (
    OID_SECA         BIGINT IDENTITY(1,1) PRIMARY KEY,
    [Source_Name]    VARCHAR(255) NULL,

    DairyName        VARCHAR(50)  NULL,
    TODAY            DATE         NULL,
    cow              VARCHAR(50)  NULL,
    stat             NVARCHAR(50) NULL,
    bd               DATE         NULL,
    typ              NVARCHAR(50) NULL,

    DryVAIdCode      NVARCHAR(20) NULL,
    DryNumLact       INT          NULL,
    DryCtrlCode      NVARCHAR(50) NULL,
    DryOffC          NVARCHAR(50) NULL,
    DryCom           NVARCHAR(50) NULL,
    DryCom2          NVARCHAR(50) NULL,
    DryCount         INT          NULL,
    DryCntLf         INT          NULL,
    DryCost          FLOAT        NULL,
    DryRevCode       NVARCHAR(50) NULL,
    DryDim           INT          NULL,
    DryAge           DECIMAL(10,2) NULL,
    DryTech          NVARCHAR(50) NULL,
    DryDat           DATE         NULL,
    DryTime          TIME(0)      NULL,
    DrySidEffL2      NVARCHAR(50) NULL,
    DryDiag          NVARCHAR(50) NULL,
    FECHAPARTO       DATE         NULL,

    [Source_FileDate] DATE        NULL,
    [Source_FileTime] TIME(3)     NULL
  );
END
GO
# SACAS
USE EVALUACION_ESTABLOS;
GO

IF OBJECT_ID('dbo.SACAS','U') IS NULL
BEGIN
  CREATE TABLE dbo.SACAS (
    OID_SACA         BIGINT IDENTITY(1,1) PRIMARY KEY,
    [Source_Name]    VARCHAR(255) NULL,

    DairyName        VARCHAR(50)  NULL,
    TODAY            DATE         NULL,
    cow              VARCHAR(50)  NULL,
    LC               INT          NULL,
    DIM              INT          NULL,
    stat             NVARCHAR(50) NULL,
    BD               DATE         NULL,
    TYP              NVARCHAR(50) NULL,

    BelHeight        DECIMAL(10,2) NULL,
    LHReas           NVARCHAR(50)  NULL,
    LHDat            DATE          NULL,
    LHType           NVARCHAR(50)  NULL,
    IMBelHeight      DECIMAL(10,2) NULL,
    BUYER            NVARCHAR(50)  NULL,

    FECHA_DE_PARTO   DATE          NULL,
    BIRDAT           DATE          NULL,
    VETCOM           NVARCHAR(50)  NULL,
    VETCOM2          NVARCHAR(50)  NULL,
    GP               NVARCHAR(50)  NULL,

    [Source_FileDate] DATE         NULL,
    [Source_FileTime] TIME(3)      NULL
  );
END
GO
# PROGENIE
USE EVALUACION_ESTABLOS;
GO

IF OBJECT_ID('dbo.PROGENIE','U') IS NULL
BEGIN
  CREATE TABLE dbo.PROGENIE (
    OID_PROGENIE      BIGINT IDENTITY(1,1) PRIMARY KEY,
    [Source_Name]     VARCHAR(255) NULL,

    DairyName         VARCHAR(50)  NULL,
    TODAY             DATE         NULL,
    COW               VARCHAR(50)  NULL,
    STAT              NVARCHAR(50) NULL,
    BD                DATE         NULL,
    TYP               NVARCHAR(50) NULL,

    OffsBirth         DATE          NULL,
    OffsETDam         NVARCHAR(50)  NULL,
    OffsETFlag        NVARCHAR(10)  NULL,
    OffsSire          NVARCHAR(100) NULL,
    Offspring         NVARCHAR(50)  NULL,
    OffsSex           NVARCHAR(10)  NULL,
    OffsValue         DECIMAL(10,2) NULL,

    [Source_FileDate] DATE         NULL,
    [Source_FileTime] TIME(3)      NULL
  );
END
GO




