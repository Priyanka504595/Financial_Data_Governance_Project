 /* Run Sections 0–3 once to set up.
For each load, run Section 4 
(truncate → import → DQ parents → promote parents → DQ children → promote children → checks).
*/

/* =========================================================
   0) DATABASE + SCHEMAS  (one-time)
   ========================================================= */
IF DB_ID('CustodyDemo') IS NULL CREATE DATABASE CustodyDemo;
GO
USE CustodyDemo;
GO
IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name='staging') EXEC('CREATE SCHEMA staging');
IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name='prod')    EXEC('CREATE SCHEMA prod');
GO

/* =========================================================
   1) STAGING TABLES (one-time)  -- using *_Staging names
   ========================================================= */
IF OBJECT_ID('staging.Accounts_Staging','U') IS NULL
CREATE TABLE staging.Accounts_Staging (
  Account_Number   VARCHAR(20),
  Investor_Name    VARCHAR(200),
  Currency_Code    VARCHAR(10),
  Account_Type     VARCHAR(50),
  Country          VARCHAR(50),
  Region           VARCHAR(20),
  Open_Date        DATE
);

IF OBJECT_ID('staging.Securities_Staging','U') IS NULL
CREATE TABLE staging.Securities_Staging (
  Security_ID      VARCHAR(20),
  Security_Name    VARCHAR(200),
  Asset_Type       VARCHAR(50),
  ISIN             VARCHAR(20),
  CUSIP            VARCHAR(20)
);

IF OBJECT_ID('staging.Transactions_Staging','U') IS NULL
CREATE TABLE staging.Transactions_Staging (
  Transaction_ID   VARCHAR(20),
  Account_Number   VARCHAR(20),
  Security_ID      VARCHAR(20),
  Trade_Date       DATE,
  Settlement_Date  DATE,
  Quantity         INT,
  Price            DECIMAL(18,2),
  Trade_Amount     DECIMAL(18,2),
  Status           VARCHAR(20),
  Currency_Code    VARCHAR(10)
);

IF OBJECT_ID('staging.Corporate_Actions_Staging','U') IS NULL
CREATE TABLE staging.Corporate_Actions_Staging (
  Action_ID        VARCHAR(20),
  Security_ID      VARCHAR(20),
  Action_Type      VARCHAR(50),
  Action_Date      DATE,
  Amount           DECIMAL(18,2)
);
GO

/* =========================================================
   2) PROD TABLES (one-time)  -- trusted layer with constraints
   ========================================================= */
IF OBJECT_ID('prod.Accounts','U') IS NULL
BEGIN
  CREATE TABLE prod.Accounts (
    Account_Number  CHAR(10)     NOT NULL PRIMARY KEY,
    Investor_Name   VARCHAR(200) NOT NULL,
    Currency_Code   CHAR(3)      NOT NULL,
    Account_Type    VARCHAR(50)  NULL,
    Country         VARCHAR(50)  NULL,
    Region          VARCHAR(20)  NULL,
    Open_Date       DATE         NULL,
    CONSTRAINT CK_Acct_Len10  CHECK (LEN(Account_Number)=10 AND Account_Number NOT LIKE '%[^0-9]%'),
    CONSTRAINT CK_Acct_Ccy3   CHECK (LEN(RTRIM(Currency_Code))=3 AND UPPER(RTRIM(Currency_Code)) NOT LIKE '%[^A-Z]%')
  );
END
GO

IF OBJECT_ID('prod.Securities','U') IS NULL
BEGIN
  CREATE TABLE prod.Securities (
    Security_ID    VARCHAR(20)  NOT NULL PRIMARY KEY,
    Security_Name  VARCHAR(200) NULL,
    Asset_Type     VARCHAR(50)  NULL,
    ISIN           VARCHAR(20)  NULL,
    CUSIP          VARCHAR(20)  NULL
  );
  IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name='IX_Securities_AssetType')
    CREATE INDEX IX_Securities_AssetType ON prod.Securities(Asset_Type);
END
GO

IF OBJECT_ID('prod.Transactions','U') IS NULL
BEGIN
  CREATE TABLE prod.Transactions (
    Transaction_ID  VARCHAR(20)   NOT NULL PRIMARY KEY,
    Account_Number  CHAR(10)      NOT NULL,
    Security_ID     VARCHAR(20)   NOT NULL,
    Trade_Date      DATE          NOT NULL,
    Settlement_Date DATE          NULL,
    Quantity        INT           NOT NULL CHECK (Quantity > 0),
    Price           DECIMAL(18,2) NOT NULL CHECK (Price >= 0),
    Trade_Amount    DECIMAL(18,2) NOT NULL CHECK (Trade_Amount >= 0),
    Status          VARCHAR(20)   NULL CHECK (Status IS NULL OR Status IN ('Settled','Failed','Pending')),
    Currency_Code   CHAR(3)       NULL CHECK (Currency_Code IS NULL OR LEN(RTRIM(Currency_Code))=3 AND UPPER(RTRIM(Currency_Code)) NOT LIKE '%[^A-Z]%'),
    CONSTRAINT FK_T_Acct FOREIGN KEY (Account_Number) REFERENCES prod.Accounts(Account_Number),
    CONSTRAINT FK_T_Sec  FOREIGN KEY (Security_ID)    REFERENCES prod.Securities(Security_ID),
    CONSTRAINT CK_T_NoFutureTrade CHECK (Trade_Date <= CAST(GETDATE() AS DATE)),
    CONSTRAINT CK_T_Settlement    CHECK (Status <> 'Settled' OR (Settlement_Date IS NOT NULL AND Settlement_Date >= Trade_Date))
  );
  IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name='IX_Trans_Account')
    CREATE INDEX IX_Trans_Account ON prod.Transactions(Account_Number);
  IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name='IX_Trans_Security')
    CREATE INDEX IX_Trans_Security ON prod.Transactions(Security_ID);
  IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name='IX_Trans_TradeDate_Status')
    CREATE INDEX IX_Trans_TradeDate_Status ON prod.Transactions(Trade_Date, Status);
END
GO

IF OBJECT_ID('prod.Corporate_Actions','U') IS NULL
BEGIN
  CREATE TABLE prod.Corporate_Actions (
    Action_ID    VARCHAR(20)   NOT NULL PRIMARY KEY,
    Security_ID  VARCHAR(20)   NOT NULL,
    Action_Type  VARCHAR(50)   NOT NULL CHECK (Action_Type IN ('Dividend','Split','Merger','SpinOff','RightsIssue')),
    Action_Date  DATE          NOT NULL CHECK (Action_Date <= CAST(GETDATE() AS DATE)),
    Amount       DECIMAL(18,2) NULL,
    CONSTRAINT FK_CA_Sec FOREIGN KEY (Security_ID) REFERENCES prod.Securities(Security_ID)
  );
  IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name='IX_CA_Sec_Date')
    CREATE INDEX IX_CA_Sec_Date ON prod.Corporate_Actions(Security_ID, Action_Date);
END
GO

/* =========================================================
   3) SHARED DQ TABLES (one-time)
   ========================================================= */
IF OBJECT_ID('staging.dq_exceptions','U') IS NULL
CREATE TABLE staging.dq_exceptions (
  Snapshot_At   DATETIME2      NOT NULL DEFAULT SYSDATETIME(),
  Source_Table  VARCHAR(64)    NOT NULL,          -- e.g., staging.Transactions_Staging
  Rule_ID       VARCHAR(50)    NOT NULL,          -- e.g., TRX_FutureTrade
  Severity      VARCHAR(10)    NOT NULL,          -- Critical/High/Med/Low
  Reason        VARCHAR(400)   NOT NULL,
  PK_1          VARCHAR(200)   NULL,              -- Business key (Txn_ID, Account_Number, etc.)
  Owner         VARCHAR(100)   NULL,              -- Team to action
  Status        VARCHAR(20)    NOT NULL DEFAULT 'Open', -- Open/Resolved/Ignored
  Details_JSON  NVARCHAR(MAX)  NULL               -- Optional (mask PII)
);

IF OBJECT_ID('staging.dq_run_log','U') IS NULL
CREATE TABLE staging.dq_run_log (
  Run_At           DATETIME2     NOT NULL DEFAULT SYSDATETIME(),
  Table_Name       VARCHAR(128)  NOT NULL,
  Rule_ID          VARCHAR(50)   NOT NULL,
  Rule_Desc        VARCHAR(200)  NOT NULL,
  Violations_Count INT           NOT NULL,
  Population_Count INT           NOT NULL,
  Violations_Pct   DECIMAL(5,2)  NOT NULL,
  Pass_Fail        VARCHAR(10)   NOT NULL         -- 'Pass'/'Fail' or 'N/A' when pop=0
);
GO

--RSL Files for PowerBI access
CREATE TABLE prod.User_Access (
  UserEmail  VARCHAR(256) NOT NULL,
  Region     VARCHAR(50)  NOT NULL
);

INSERT INTO prod.User_Access (UserEmail, Region) VALUES
('alice@yourco.com','LATAM'),
('bob@yourco.com','EMEA'),
('carol@yourco.com','APAC');

/* =========================================================
   4) >>> PER LOAD STARTS HERE <<<
      TRUNCATE STAGING, IMPORT CSVs, DQ-GATED SEED PARENTS,
      DQ CHILDREN, PROMOTE CLEAN CHILDREN, CHECKS
   ========================================================= */

-- A) Clear old staging (skip if appending by batch key)
TRUNCATE TABLE staging.Accounts_Staging;
TRUNCATE TABLE staging.Securities_Staging;
TRUNCATE TABLE staging.Transactions_Staging;
TRUNCATE TABLE staging.Corporate_Actions_Staging;

-- B) Import your four CSVs (wizard OR BULK INSERT)
BULK INSERT staging.Accounts_Staging
FROM 'C:\Users\priya\Source\Repos\FinData_Governance_Project\Accounts.csv'
WITH (FIRSTROW=2, FIELDTERMINATOR=',', ROWTERMINATOR='0x0D0A', FIELDQUOTE='"', CODEPAGE='65001', KEEPNULLS, TABLOCK);

BULK INSERT staging.Securities_Staging
FROM 'C:\Users\priya\Source\Repos\FinData_Governance_Project\Securities.csv'
WITH (FIRSTROW=2, FIELDTERMINATOR=',', ROWTERMINATOR='0x0D0A', FIELDQUOTE='"', CODEPAGE='65001', KEEPNULLS, TABLOCK);

BULK INSERT staging.Transactions_Staging
FROM 'C:\Users\priya\Source\Repos\FinData_Governance_Project\Transactions.csv'
WITH (FIRSTROW=2, FIELDTERMINATOR=',', ROWTERMINATOR='0x0D0A', FIELDQUOTE='"', CODEPAGE='65001', KEEPNULLS, TABLOCK);

BULK INSERT staging.Corporate_Actions_Staging
FROM 'C:\Users\priya\Source\Repos\FinData_Governance_Project\Corporate_Actions.csv'
WITH (FIRSTROW=2, FIELDTERMINATOR=',', ROWTERMINATOR='0x0D0A', FIELDQUOTE='"', CODEPAGE='65001', KEEPNULLS, TABLOCK);

-- Quick Check After load
SELECT 'Accounts', COUNT(*) FROM staging.Accounts_Staging
UNION ALL SELECT 'Securities', COUNT(*) FROM staging.Securities_Staging
UNION ALL SELECT 'Transactions', COUNT(*) FROM staging.Transactions_Staging
UNION ALL SELECT 'Corp_Actions', COUNT(*) FROM staging.Corporate_Actions_Staging;

Select Top(2) * from staging.Accounts_Staging
Select Top(2) * from staging.Securities_Staging
Select Top(2) * from staging.Transactions_Staging
Select Top(2) * from staging.Corporate_Actions_Staging

-- C) Run DQ on PARENTS (Accounts & Securities) and log run-logs
DECLARE @RunAt DATETIME2 = SYSDATETIME();

---------------------------
-- ACCOUNTS_Staging (exceptions)
---------------------------
INSERT INTO staging.dq_exceptions (Snapshot_At,Source_Table,Rule_ID,Severity,Reason,PK_1,Owner)
SELECT @RunAt,'staging.Accounts_Staging','ACCT_BadLen','High','Account_Number not 10 digits', a.Account_Number,'Account Master'
FROM staging.Accounts_Staging a
WHERE LEN(a.Account_Number) <> 10 OR a.Account_Number LIKE '%[^0-9]%';

INSERT INTO staging.dq_exceptions (Snapshot_At,Source_Table,Rule_ID,Severity,Reason,PK_1,Owner)
SELECT @RunAt,'staging.Accounts_Staging','ACCT_CurrencyFmt','Medium','Currency_Code not ISO-like 3 letters', a.Account_Number,'Account Master'
FROM staging.Accounts_Staging a
WHERE a.Currency_Code IS NULL
   OR LEN(RTRIM(a.Currency_Code)) <> 3
   OR UPPER(RTRIM(a.Currency_Code)) LIKE '%[^A-Z]%';

-- ACCOUNTS_Staging (run-log safe)
DECLARE @pop_A INT  = (SELECT COUNT(*) FROM staging.Accounts_Staging);
DECLARE @viol_A INT = (SELECT COUNT(DISTINCT PK_1) FROM staging.dq_exceptions WHERE Source_Table='staging.Accounts_Staging' AND Snapshot_At=@RunAt);
DECLARE @pct_A  DECIMAL(5,2) = (CASE WHEN @pop_A=0 THEN 0 ELSE CAST(100.0*@viol_A/@pop_A AS DECIMAL(5,2)) END);
DECLARE @pf_A   VARCHAR(10)  = (CASE WHEN @pop_A=0 THEN 'N/A' WHEN @viol_A=0 THEN 'Pass' ELSE 'Fail' END);

INSERT INTO staging.dq_run_log (Run_At,Table_Name,Rule_ID,Rule_Desc,Violations_Count,Population_Count,Violations_Pct,Pass_Fail)
VALUES (@RunAt,'staging.Accounts_Staging','ACCT_All','10-digit numeric + ISO3', @viol_A,@pop_A,@pct_A,@pf_A);

---------------------------
-- SECURITIES_Staging (exceptions)
---------------------------
INSERT INTO staging.dq_exceptions (Snapshot_At,Source_Table,Rule_ID,Severity,Reason,PK_1,Owner)
SELECT @RunAt,'staging.Securities_Staging','SEC_NullID','Critical','Security_ID is null','(null)','Securities Master'
FROM staging.Securities_Staging WHERE Security_ID IS NULL;

INSERT INTO staging.dq_exceptions (Snapshot_At,Source_Table,Rule_ID,Severity,Reason,PK_1,Owner)
SELECT @RunAt,'staging.Securities_Staging','SEC_DupID','High','Duplicate Security_ID', s.Security_ID,'Securities Master'
FROM staging.Securities_Staging s
JOIN (SELECT Security_ID FROM staging.Securities_Staging GROUP BY Security_ID HAVING COUNT(*)>1) d
  ON d.Security_ID = s.Security_ID;

-- SECURITIES_Staging (run-log safe)
DECLARE @pop_S INT  = (SELECT COUNT(*) FROM staging.Securities_Staging);
DECLARE @viol_S INT = (SELECT COUNT(DISTINCT PK_1) FROM staging.dq_exceptions WHERE Source_Table='staging.Securities_Staging' AND Snapshot_At=@RunAt);
DECLARE @pct_S  DECIMAL(5,2) = (CASE WHEN @pop_S=0 THEN 0 ELSE CAST(100.0*@viol_S/@pop_S AS DECIMAL(5,2)) END);
DECLARE @pf_S   VARCHAR(10)  = (CASE WHEN @pop_S=0 THEN 'N/A' WHEN @viol_S=0 THEN 'Pass' ELSE 'Fail' END);

INSERT INTO staging.dq_run_log (Run_At,Table_Name,Rule_ID,Rule_Desc,Violations_Count,Population_Count,Violations_Pct,Pass_Fail)
VALUES (@RunAt,'staging.Securities_Staging','SEC_All','Security_ID present & unique', @viol_S,@pop_S,@pct_S,@pf_S);

-- D) DQ-GATED SEEDING of PARENTS into PROD (only rows NOT in exceptions at @RunAt)
INSERT INTO prod.Accounts (Account_Number, Investor_Name, Currency_Code, Account_Type, Country, Region, Open_Date)
SELECT LEFT(a.Account_Number,10), a.Investor_Name, LEFT(a.Currency_Code,3),
       a.Account_Type, a.Country, a.Region, a.Open_Date
FROM staging.Accounts_Staging a
WHERE NOT EXISTS (
        SELECT 1 FROM staging.dq_exceptions e
        WHERE e.Source_Table='staging.Accounts_Staging'
          AND e.PK_1 = a.Account_Number
          AND e.Snapshot_At = @RunAt
      )
  AND LEN(a.Account_Number)=10 AND a.Account_Number NOT LIKE '%[^0-9]%'
  AND a.Currency_Code IS NOT NULL
  AND LEN(RTRIM(a.Currency_Code))=3
  AND UPPER(RTRIM(a.Currency_Code)) NOT LIKE '%[^A-Z]%'
  AND NOT EXISTS (SELECT 1 FROM prod.Accounts p WHERE p.Account_Number = LEFT(a.Account_Number,10));

;WITH SEC_SRC AS (
  SELECT s.*, ROW_NUMBER() OVER (PARTITION BY s.Security_ID ORDER BY s.Security_Name) AS rn
  FROM staging.Securities_Staging s
  WHERE s.Security_ID IS NOT NULL
)
INSERT INTO prod.Securities (Security_ID, Security_Name, Asset_Type, ISIN, CUSIP)
SELECT s.Security_ID, s.Security_Name, s.Asset_Type, s.ISIN, s.CUSIP
FROM SEC_SRC s
WHERE s.rn = 1
  AND NOT EXISTS (
        SELECT 1 FROM staging.dq_exceptions e
        WHERE e.Source_Table='staging.Securities_Staging'
          AND e.PK_1 = s.Security_ID
          AND e.Snapshot_At = @RunAt
      )
  AND NOT EXISTS (SELECT 1 FROM prod.Securities p WHERE p.Security_ID = s.Security_ID);

-- E) DQ CHILDREN (Transactions & Corporate_Actions) and log run-logs
---------------------------
-- TRANSACTIONS_Staging (exceptions)
---------------------------
INSERT INTO staging.dq_exceptions (Snapshot_At,Source_Table,Rule_ID,Severity,Reason,PK_1,Owner)
SELECT @RunAt,'staging.Transactions_Staging','TRX_FutureTrade','Critical','Trade_Date in future', t.Transaction_ID,'Trading Ops'
FROM staging.Transactions_Staging t
WHERE t.Trade_Date > CAST(GETDATE() AS DATE);

INSERT INTO staging.dq_exceptions (Snapshot_At,Source_Table,Rule_ID,Severity,Reason,PK_1,Owner)
SELECT @RunAt,'staging.Transactions_Staging','TRX_BadSettlement','High','Settled but Settlement_Date null or < Trade_Date', t.Transaction_ID,'Settlement Ops'
FROM staging.Transactions_Staging t
WHERE t.Status='Settled' AND (t.Settlement_Date IS NULL OR t.Settlement_Date < t.Trade_Date);

INSERT INTO staging.dq_exceptions (Snapshot_At,Source_Table,Rule_ID,Severity,Reason,PK_1,Owner)
SELECT @RunAt,'staging.Transactions_Staging','TRX_NegAmount','High','Trade_Amount < 0', t.Transaction_ID,'Trading Ops'
FROM staging.Transactions_Staging t WHERE t.Trade_Amount < 0;

INSERT INTO staging.dq_exceptions (Snapshot_At,Source_Table,Rule_ID,Severity,Reason,PK_1,Owner)
SELECT @RunAt,'staging.Transactions_Staging','TRX_NonPosQty','High','Quantity <= 0', t.Transaction_ID,'Trading Ops'
FROM staging.Transactions_Staging t WHERE t.Quantity <= 0;

INSERT INTO staging.dq_exceptions (Snapshot_At,Source_Table,Rule_ID,Severity,Reason,PK_1,Owner)
SELECT @RunAt,'staging.Transactions_Staging','TRX_AcctLen','High','Account_Number not 10 digits', t.Transaction_ID,'Account Master'
FROM staging.Transactions_Staging t WHERE LEN(t.Account_Number) <> 10 OR t.Account_Number LIKE '%[^0-9]%';

INSERT INTO staging.dq_exceptions (Snapshot_At,Source_Table,Rule_ID,Severity,Reason,PK_1,Owner)
SELECT @RunAt,'staging.Transactions_Staging','TRX_CurrencyFmt','Medium','Currency_Code not ISO-like 3 letters', t.Transaction_ID,'Trading Ops'
FROM staging.Transactions_Staging t
WHERE t.Currency_Code IS NULL OR LEN(RTRIM(t.Currency_Code))<>3 OR UPPER(RTRIM(t.Currency_Code)) LIKE '%[^A-Z]%';

INSERT INTO staging.dq_exceptions (Snapshot_At,Source_Table,Rule_ID,Severity,Reason,PK_1,Owner)
SELECT @RunAt,'staging.Transactions_Staging','TRX_MissingAccount','High','Account FK not found', t.Transaction_ID,'Account Master'
FROM staging.Transactions_Staging t
LEFT JOIN prod.Accounts a ON a.Account_Number = LEFT(t.Account_Number,10)
WHERE a.Account_Number IS NULL;

INSERT INTO staging.dq_exceptions (Snapshot_At,Source_Table,Rule_ID,Severity,Reason,PK_1,Owner)
SELECT @RunAt,'staging.Transactions_Staging','TRX_MissingSecurity','High','Security FK not found', t.Transaction_ID,'Securities Master'
FROM staging.Transactions_Staging t
LEFT JOIN prod.Securities s ON s.Security_ID = t.Security_ID
WHERE s.Security_ID IS NULL;

INSERT INTO staging.dq_exceptions (Snapshot_At,Source_Table,Rule_ID,Severity,Reason,PK_1,Owner)
SELECT @RunAt,'staging.Transactions_Staging','TRX_DupTxnID','Critical','Duplicate Transaction_ID', t.Transaction_ID,'Trading Ops'
FROM staging.Transactions_Staging t
JOIN (SELECT Transaction_ID FROM staging.Transactions_Staging GROUP BY Transaction_ID HAVING COUNT(*)>1) d
  ON d.Transaction_ID = t.Transaction_ID;

-- TRANSACTIONS_Staging (run-log safe)
DECLARE @pop_T INT  = (SELECT COUNT(*) FROM staging.Transactions_Staging);
DECLARE @viol_T INT = (SELECT COUNT(DISTINCT PK_1) FROM staging.dq_exceptions WHERE Source_Table='staging.Transactions_Staging' AND Snapshot_At=@RunAt);
DECLARE @pct_T  DECIMAL(5,2) = (CASE WHEN @pop_T=0 THEN 0 ELSE CAST(100.0*@viol_T/@pop_T AS DECIMAL(5,2)) END);
DECLARE @pf_T   VARCHAR(10)  = (CASE WHEN @pop_T=0 THEN 'N/A' WHEN @viol_T=0 THEN 'Pass' ELSE 'Fail' END);

INSERT INTO staging.dq_run_log (Run_At,Table_Name,Rule_ID,Rule_Desc,Violations_Count,Population_Count,Violations_Pct,Pass_Fail)
VALUES (@RunAt,'staging.Transactions_Staging','TRX_All',
        'No future; settle ok; amount>=0; qty>0; acct len; ISO3; FKs; no dup',
        @viol_T,@pop_T,@pct_T,@pf_T);

---------------------------
-- CORPORATE_ACTIONS_Staging (exceptions)
---------------------------
INSERT INTO staging.dq_exceptions (Snapshot_At,Source_Table,Rule_ID,Severity,Reason,PK_1,Owner)
SELECT @RunAt,'staging.Corporate_Actions_Staging','CA_FutureDate','Medium','Action_Date in future', c.Action_ID,'Corp Actions'
FROM staging.Corporate_Actions_Staging c
WHERE c.Action_Date > CAST(GETDATE() AS DATE);

INSERT INTO staging.dq_exceptions (Snapshot_At,Source_Table,Rule_ID,Severity,Reason,PK_1,Owner)
SELECT @RunAt,'staging.Corporate_Actions_Staging','CA_MissingSecurity','High','Security FK not found', c.Action_ID,'Securities Master'
FROM staging.Corporate_Actions_Staging c
LEFT JOIN prod.Securities s ON s.Security_ID = c.Security_ID
WHERE s.Security_ID IS NULL;

INSERT INTO staging.dq_exceptions (Snapshot_At,Source_Table,Rule_ID,Severity,Reason,PK_1,Owner)
SELECT @RunAt,'staging.Corporate_Actions_Staging','CA_BadType','High','Action_Type not in allowed list', c.Action_ID,'Corp Actions'
FROM staging.Corporate_Actions_Staging c
WHERE c.Action_Type NOT IN ('Dividend','Split','Merger','SpinOff','RightsIssue');

-- CORPORATE_ACTIONS_Staging (run-log safe)
DECLARE @pop_C INT  = (SELECT COUNT(*) FROM staging.Corporate_Actions_Staging);
DECLARE @viol_C INT = (SELECT COUNT(DISTINCT PK_1) FROM staging.dq_exceptions WHERE Source_Table='staging.Corporate_Actions_Staging' AND Snapshot_At=@RunAt);
DECLARE @pct_C  DECIMAL(5,2) = (CASE WHEN @pop_C=0 THEN 0 ELSE CAST(100.0*@viol_C/@pop_C AS DECIMAL(5,2)) END);
DECLARE @pf_C   VARCHAR(10)  = (CASE WHEN @pop_C=0 THEN 'N/A' WHEN @viol_C=0 THEN 'Pass' ELSE 'Fail' END);

INSERT INTO staging.dq_run_log (Run_At,Table_Name,Rule_ID,Rule_Desc,Violations_Count,Population_Count,Violations_Pct,Pass_Fail)
VALUES (@RunAt,'staging.Corporate_Actions_Staging','CA_All','No future; FK ok; type domain', @viol_C,@pop_C,@pct_C,@pf_C);

-- F) PROMOTE CLEAN CHILDREN (Transactions & Corporate_Actions) for this @RunAt
SET XACT_ABORT ON;
BEGIN TRAN;

INSERT INTO prod.Transactions
(Transaction_ID, Account_Number, Security_ID, Trade_Date, Settlement_Date, Quantity, Price, Trade_Amount, Status, Currency_Code)
SELECT t.Transaction_ID, LEFT(t.Account_Number,10), t.Security_ID, t.Trade_Date, t.Settlement_Date,
       t.Quantity, t.Price, t.Trade_Amount, t.Status, LEFT(t.Currency_Code,3)
FROM staging.Transactions_Staging t
WHERE NOT EXISTS (
        SELECT 1 FROM staging.dq_exceptions e
        WHERE e.Source_Table='staging.Transactions_Staging'
          AND e.PK_1 = t.Transaction_ID
          AND e.Snapshot_At = @RunAt
      )
  AND NOT EXISTS (SELECT 1 FROM prod.Transactions p WHERE p.Transaction_ID = t.Transaction_ID);

INSERT INTO prod.Corporate_Actions (Action_ID, Security_ID, Action_Type, Action_Date, Amount)
SELECT c.Action_ID, c.Security_ID, c.Action_Type, c.Action_Date, c.Amount
FROM staging.Corporate_Actions_Staging c
WHERE NOT EXISTS (
        SELECT 1 FROM staging.dq_exceptions e
        WHERE e.Source_Table='staging.Corporate_Actions_Staging'
          AND e.PK_1 = c.Action_ID
          AND e.Snapshot_At = @RunAt
      )
  AND NOT EXISTS (SELECT 1 FROM prod.Corporate_Actions p WHERE p.Action_ID = c.Action_ID);

COMMIT;

-- G) Sanity checks
SELECT 'Accounts' AS T, COUNT(*) AS Rows FROM prod.Accounts
UNION ALL SELECT 'Securities', COUNT(*) FROM prod.Securities
UNION ALL SELECT 'Transactions', COUNT(*) FROM prod.Transactions
UNION ALL SELECT 'Corporate_Actions', COUNT(*) FROM prod.Corporate_Actions;

SELECT COUNT(*) AS Bad_Account_FK
FROM prod.Transactions t
LEFT JOIN prod.Accounts a ON a.Account_Number = t.Account_Number
WHERE a.Account_Number IS NULL;

SELECT COUNT(*) AS Bad_Security_FK
FROM prod.Transactions t
LEFT JOIN prod.Securities s ON s.Security_ID = t.Security_ID
WHERE s.Security_ID IS NULL;

SELECT COUNT(*) AS Dup_Txn
FROM prod.Transactions
GROUP BY Transaction_ID
HAVING COUNT(*) > 1;

/* See the latest run-log (Pass/Fail and % by table) */
-- Latest DQ run summary
SELECT TOP 100 *
FROM staging.dq_run_log
ORDER BY Run_At DESC, Table_Name;
/* See what (if anything) failed in the last run */
-- Violations by table/rule for the latest run 
DECLARE @latest DATETIME2 = (SELECT MAX(Snapshot_At) FROM staging.dq_exceptions);
SELECT Source_Table, Rule_ID, Severity, COUNT(*) AS Violations
FROM staging.dq_exceptions
WHERE Snapshot_At = @latest
GROUP BY Source_Table, Rule_ID, Severity
ORDER BY Source_Table, Violations DESC, Severity DESC;

-- Peek a few example bad rows (change table/rule as needed)
SELECT TOP 10 *
FROM staging.dq_exceptions
WHERE Snapshot_At = @latest
ORDER BY Severity DESC, Rule_ID;

/* Double-check duplicates in prod */
SELECT COUNT(*) AS Dup_Txn
FROM prod.Transactions
GROUP BY Transaction_ID
HAVING COUNT(*) > 1;





-- Invalid Changes Check

-- Insert passes the Settlement check, but has an invalid currency 'XXA'
INSERT INTO staging.Transactions_Staging
( Transaction_ID, Account_Number, Security_ID, Trade_Date, Settlement_Date,
  Quantity, Price, Trade_Amount, Status, Currency_Code )
VALUES
( 999998, '00120894', 'SEC00001',
  '2025-09-22',
  '2025-09-20',   
  100, 10.00, 1000.00,
  'Settled',
  'CCA');         -- invalid (should be flagged by DQ)


Select * FROM staging.Transactions_Staging
WHERE Transaction_ID = '999998';

--  quick delete
DELETE FROM staging.Transactions_Staging
WHERE Transaction_ID = '999998';


-- Verify it's gone
SELECT * FROM staging.dq_exceptions;
SELECT * FROM staging.dq_run_log;

DELETE FROM staging.dq_exceptions;
DELETE FROM staging.dq_run_log;