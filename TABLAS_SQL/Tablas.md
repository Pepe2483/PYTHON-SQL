# Crear tablas en Sql sin espacion ni putnos en los nombre
# 1) PARTOS

USE EVALUACION_ESTABLOS;
GO


IF OBJECT_ID('dbo.PARTOS','U') IS NULL
BEGIN
  CREATE TABLE dbo.PARTOS (
  
    OID_PARTO          INT IDENTITY(1,1) PRIMARY KEY,
    Source_Name      VARCHAR(80) NULL,
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
# 2) ABORTOS
IF OBJECT_ID('dbo.ABORTOS','U') IS NULL
BEGIN
  CREATE TABLE dbo.ABORTOS (
  
    OID_ABORTO         INT IDENTITY(1,1) PRIMARY KEY,
    Source_Name      VARCHAR(80) NULL,
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

# 3) SECADO


IF OBJECT_ID('dbo.SECADO','U') IS NULL
BEGIN
  CREATE TABLE dbo.SECADO (

  
    OID_SECA           INT IDENTITY(1,1) PRIMARY KEY,
    Source_Name      VARCHAR(80) NULL,
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


# 4) SACAS
USE EVALUACION_ESTABLOS;
GO

IF OBJECT_ID('dbo.SACAS','U') IS NULL
BEGIN
  CREATE TABLE dbo.SACAS (
  
    OID_SACA         BIGINT IDENTITY(1,1) PRIMARY KEY,
    Source_Name    VARCHAR(255) NULL,

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

    Source_FileDate DATE         NULL,
    Source_FileTime TIME(3)      NULL
  );
END
GO
# 5) PROGENIE
USE EVALUACION_ESTABLOS;
GO

IF OBJECT_ID('dbo.PROGENIE','U') IS NULL
BEGIN
  CREATE TABLE dbo.PROGENIE (
  
    OID_PROGENIE      BIGINT IDENTITY(1,1) PRIMARY KEY,
    Source_Name     VARCHAR(255) NULL,

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

    Source_FileDate DATE         NULL,
    Source_FileTime TIME(3)      NULL
  );
END
GO

# 6) NACIMIENTOS
USE EVALUACION_ESTABLOS;
GO

IF OBJECT_ID('dbo.NACIMIENTOS','U') IS NULL
BEGIN
  CREATE TABLE dbo.NACIMIENTOS (
  
    OID_NACIMIENTO    BIGINT IDENTITY(1,1) PRIMARY KEY,
    Source_Name     VARCHAR(255) NULL,

    DairyName         VARCHAR(50)  NULL,
    TODAY             DATE         NULL,
    cow               VARCHAR(50)  NULL,
    stat              NVARCHAR(50) NULL,
    bd                DATE         NULL,
    typ               NVARCHAR(50) NULL,

    BirVAIdCode       NVARCHAR(20) NULL,
    BirNumLact        INT          NULL,
    BirCtrlCode       NVARCHAR(50) NULL,
    BirOffC           NVARCHAR(50) NULL,
    BirCom            NVARCHAR(50) NULL,
    BirCom2           NVARCHAR(50) NULL,
    BirCount          INT          NULL,
    BirCntLf          INT          NULL,
    BirCost           FLOAT        NULL,
    BirRevCode        NVARCHAR(50) NULL,
    BirDim            INT          NULL,
    BirAge            DECIMAL(10,2) NULL,
    BirTech           NVARCHAR(50) NULL,
    BirClvEase        NVARCHAR(50) NULL,
    BirClvEaseCod     NVARCHAR(20) NULL,
    BirDat            DATE         NULL,
    BirTime           TIME(0)      NULL,
    BirSidEffL2       NVARCHAR(50) NULL,
    BirDiag           NVARCHAR(50) NULL,

    Source_FileDate DATE         NULL,
    Source-FileTime TIME(3)      NULL
  );
END
GO
# 7) ENFERMEDADES

USE EVALUACION_ESTABLOS;
GO

IF OBJECT_ID('dbo.ENFERMEDADES','U') IS NULL
BEGIN
  CREATE TABLE dbo.ENFERMEDADES (
  
    OID_ENFERMEDAD    BIGINT IDENTITY(1,1) PRIMARY KEY,
    Source_Name     VARCHAR(255) NULL,

    DairyName         VARCHAR(50)  NULL,
    TODAY             DATE         NULL,
    cow               VARCHAR(50)  NULL,
    stat              NVARCHAR(50) NULL,
    BD                DATE         NULL,
    TYP               NVARCHAR(50) NULL,

    VetVAIdCode       NVARCHAR(20) NULL,
    lc                INT          NULL,
    VetNumLact        INT          NULL,
    VetAmount         FLOAT        NULL,
    VetReason         NVARCHAR(50) NULL,
    VetCtrlCode       NVARCHAR(50) NULL,
    VetCount3Day      INT          NULL,
    VetActionType     NVARCHAR(50) NULL,
    VetOffC           NVARCHAR(50) NULL,
    VetCom            NVARCHAR(50) NULL,
    VetCom2           NVARCHAR(50) NULL,
    VetCount          INT          NULL,
    VetCntLf          INT          NULL,
    VetCost           FLOAT        NULL,
    VetDosageUnit     NVARCHAR(50) NULL,
    VetRecommDose     NVARCHAR(50) NULL,
    VetDim            INT          NULL,
    VetAge            DECIMAL(10,2) NULL,
    VetTech           NVARCHAR(50) NULL,
    VetDat            DATE         NULL,
    VetFrequency      NVARCHAR(50) NULL,
    VetTime           TIME(0)      NULL,
    Vet2Do            INT          NULL,
    VetMetCode        NVARCHAR(50) NULL,
    VetName           NVARCHAR(100) NULL,
    VetFullName       NVARCHAR(100) NULL,
    VetXCode          NVARCHAR(50) NULL,
    VetAct2hA         NVARCHAR(50) NULL,
    VetDiag           NVARCHAR(50) NULL,
    FECHAPARTO        DATE         NULL,

    Source_FileDate DATE         NULL,
    Source_FileTime TIME(3)      NULL
  );
END
GO


  # 8) TEST_DE_PRENEZ
  

USE EVALUACION_ESTABLOS;
GO


IF OBJECT_ID('dbo.TEST_DE_PRENEZ','U') IS NULL
BEGIN
  CREATE TABLE dbo.TEST_DE_PRENEZ (
  
    OID_TEST_PRENEZ   BIGINT IDENTITY(1,1) PRIMARY KEY,
    Source_Name     VARCHAR(255) NULL,

    DairyName         VARCHAR(50)  NULL,
    [FECHA DE ARCHIVO] DATE        NULL,
    COW               VARCHAR(50)  NULL,
    LC_ACTUAL       INT          NULL,
    DIM_ACTUAL     INT          NULL,
    STAT_ACTUAL     NVARCHAR(50) NULL,
    RAZA              NVARCHAR(10) NULL,
    SEXO              NVARCHAR(10) NULL,

    PrgVAIdCode       NVARCHAR(20) NULL,
    PrgNumLact        INT          NULL,
    PrgCtrlCode       NVARCHAR(50) NULL,
    PrgOffC           NVARCHAR(50) NULL,
    PrgCom            NVARCHAR(50) NULL,
    PrgCom2           NVARCHAR(50) NULL,
    PrgCount          INT          NULL,
    PrgCntLf          INT          NULL,
    PrgCost           FLOAT        NULL,
    PrgRevCode        NVARCHAR(50) NULL,
    PrgDim            INT          NULL,
    PrgAge            DECIMAL(10,2) NULL,
    PrgTech           NVARCHAR(50) NULL,
    PrgDat            DATE         NULL,
    PrgTime           TIME(0)      NULL,
    Prg2Do            INT          NULL,
    PrgSidEffL2       NVARCHAR(50) NULL,
    PrgDiag           NVARCHAR(50) NULL,
    FECHA_DE_PARTO  DATE         NULL,

    Source_FileDate DATE         NULL,
    Source_FileTime TIME(3)      NULL
  );
END
GO


   # 9) GENERALES_RECRIA
  
IF OBJECT_ID('dbo.GENERALES_RECRIA','U') IS NULL
BEGIN
  CREATE TABLE dbo.GENERALES_RECRIA (
  
    OID_GENERAL_RECRIA BIGINT IDENTITY(1,1) PRIMARY KEY,
    Source_Name     VARCHAR(255) NULL,

    DairyName          VARCHAR(50)  NULL,
    today          DATE         NULL,
    HSDate             DATE         NULL,

    HSAnimalCountTotal_0@YF FLOAT NULL,
    HSCountAnimElig_0@Y    FLOAT NULL,
    HSInsemCountTotal@Y   FLOAT NULL,
    HSInsemCountSuccess@Y   FLOAT NULL,
    HSInsemRateCycle@Y      FLOAT NULL,
    HSPregRateCycle@Y      FLOAT NULL,
    HSCountPreg@Y           FLOAT NULL,
    HSAnimalCountLeft@yF   FLOAT NULL,

    Source_FileDate DATE    NULL,
    Source_FileTime TIME(3) NULL
  );
END
GO


   # 10) GENERALES_VACAS
 
IF OBJECT_ID('dbo.GENERALES_VACAS','U') IS NULL
BEGIN
  CREATE TABLE dbo.GENERALES_VACAS (
  
    OID_GENERAL_VACA  BIGINT IDENTITY(1,1) PRIMARY KEY,
    Source_Name     VARCHAR(255) NULL,

    DairyName         VARCHAR(50)  NULL,
    today       DATE         NULL,
    HSDate            DATE         NULL,

    HSAnimalCountTotal_0@1p FLOAT NULL,
    HSAnimalCountMilking_0 FLOAT NULL,
    HSCountAnimElig_0       FLOAT NULL,
    HSInsemCountTotal@1p   FLOAT NULL,
    HSInsemSuccess@1p       FLOAT NULL,
    HSPregRateCycle@1p      FLOAT NULL,
    HSCountPreg@1p_0       FLOAT NULL,
    HSAvgDIMFirstInsem@1p   FLOAT NULL,
    HSAvgDaysOpen@1p        FLOAT NULL,
    HSAvgCalvingInterval    FLOAT NULL,
    HSAvgPeakMilk@1p        FLOAT NULL,
    HSAvgPeakMilkDIM        FLOAT NULL,
    HSAvgDailyMilk          FLOAT NULL,
    HSRollingHerdAvg@1p     FLOAT NULL,
    HSRollingHerdAvg@1      FLOAT NULL,
    HSRollingHerdAvg@2p    FLOAT NULL,
    HSAnimalCountLeft@1P    FLOAT NULL,

    Source_FileDate DATE    NULL,
    Source-FileTime TIME(3) NULL
  );
END
GO


   # 11) GENERALES_ESTABLOS
   
IF OBJECT_ID('dbo.GENERALES_ESTABLOS','U') IS NULL
BEGIN
  CREATE TABLE dbo.GENERALES_ESTABLOS (
  
    OID_GENERAL_ESTABLO BIGINT IDENTITY(1,1) PRIMARY KEY,
    Source_Name       VARCHAR(255) NULL,

    DairyName           VARCHAR(50)  NULL,
    today          DATE         NULL,
    HSDate              DATE         NULL,

    HSAnimalCountTotal_0@1p FLOAT NULL,
    HSAnimalCountTotal@yf_0 FLOAT NULL,
    HSAnimalCountTotal@1p_0 FLOAT NULL,
    HSAnimalCountTotal@2_0  FLOAT NULL,
    HSAnimalCountTotal@3p_0 FLOAT NULL,
    HSAnimalCountMilking_0  FLOAT NULL,
    HSAnimalCountMilking@1_0 FLOAT NULL,
    HSAnimalCountMilking@2_0 FLOAT NULL,
    HSAnimalCountMilking@3p_0 FLOAT NULL,

    HSAvgDIM_0        FLOAT NULL,
    HSAvgDIM_1@1_0    FLOAT NULL,
    HSAvgDIM_1@2_0    FLOAT NULL,
    HSAvgDIM_1@3p_0   FLOAT NULL,
    HSAvgDIMFirstInsem FLOAT NULL,
    HSAvgDaysOpen      FLOAT NULL,
    HSAvgCalvingInterval_30_5 FLOAT NULL,
    HSAvgCalvingAgeMonths@1 FLOAT NULL,
    HSAnimalCountLeft@1P FLOAT NULL,

    Source_FileDate DATE    NULL,
    Source_FileTime TIME(3) NULL
  );
END
GO


   # 12) HATO_ACTUAL
   
IF OBJECT_ID('dbo.HATO_ACTUAL','U') IS NULL
BEGIN
  CREATE TABLE dbo.HATO_ACTUAL (
  
    OID_HATO_ACTUAL   BIGINT IDENTITY(1,1) PRIMARY KEY,
    Source_Name     VARCHAR(255) NULL,

    DairyName         VARCHAR(50)  NULL,
    TODAY        DATE         NULL,
    cow               VARCHAR(50)  NULL,
    gp                NVARCHAR(50) NULL,
    lc                INT          NULL,
    dim               INT          NULL,
    stat              NVARCHAR(50) NULL,
    statprod          NVARCHAR(50) NULL,
    bd                DATE         NULL,
    typ               NVARCHAR(50) NULL,
    DYP               NVARCHAR(50) NULL,
    BIRDAT            DATE         NULL,
    AGEM              INT          NULL,
    CLVDAT            DATE         NULL,
    SrvDat            DATE         NULL,
    SRVSIRE           NVARCHAR(100) NULL,
    SrvCount          INT          NULL,
    SReg              NVARCHAR(50)  NULL,
    SIRE              NVARCHAR(100) NULL,
    ASReg1            NVARCHAR(50)  NULL,
    DAMBN             NVARCHAR(50)  NULL,
    DamSire_30      NVARCHAR(50)  NULL,
    DamBd             DATE          NULL,

    avmlk             FLOAT        NULL,
    ACMLK             FLOAT        NULL,
    ACMLK_1       FLOAT        NULL,
    ACMLK_2       FLOAT        NULL,
    TestMilk          FLOAT        NULL,
    TestMilk_1    FLOAT        NULL,
    TestMilk_2    FLOAT        NULL,
    m200              FLOAT        NULL,
    m305              FLOAT        NULL,
    m365              FLOAT        NULL,
    DryDat            DATE         NULL,
    RcDry             NVARCHAR(50) NULL,
    FECHAPARTOFUTURO  DATE         NULL,

    Source_FileDate DATE         NULL,
    Source_FileTime TIME(3)      NULL
  );
END
GO


 # 13) LACTACIONES
  
IF OBJECT_ID('dbo.LACTACIONES','U') IS NULL
BEGIN
  CREATE TABLE dbo.LACTACIONES (
  
    OID_LACTACIONES   BIGINT IDENTITY(1,1) PRIMARY KEY,
    Source_Name     VARCHAR(255) NULL,

    DairyName         VARCHAR(50)  NULL,
    FECHA_ARCHIVO   DATE         NULL,
    COW               VARCHAR(50)  NULL,
    STAT_ACTUAL     NVARCHAR(50) NULL,
    PDCalve           NVARCHAR(50) NULL,
    LactNo            INT          NULL,
    LDim              INT          NULL,
    Calf1             NVARCHAR(50) NULL,
    Calf2             NVARCHAR(50) NULL,
    DFC               INT          NULL,
    FC                DATE         NULL,
    MC                DATE         NULL,
    DMC               DATE         NULL,
    DC                DATE         NULL,
    LactM             FLOAT        NULL,
    M100              FLOAT        NULL,
    M200              FLOAT        NULL,
    M305              FLOAT        NULL,
    CalfNum           INT          NULL,
    Calf2Num          INT          NULL,
    CalfSex2          NVARCHAR(10) NULL,
    CalfSex           NVARCHAR(10) NULL,
    CI                FLOAT        NULL,

    Source_FileDate DATE         NULL,
    Source_FileTime TIME(3)      NULL
  );
END
GO









