/**********************************************************************************************************************************************
Purpose: This script breaks out the reporting portfolios for US Hurricane exposures in RiskLink.
Author: Charlene Chia
Instructions: 
				1. Update quarter e.g. 202212 to 202306. Use Replace All feature.
				2. Update EDM database.
				3. Confirm that there are no changes to the LOBs within User definied fields in the accgrp table. See commented section below.
				4. Update Portinfoid. See commented section below.
				5. Update Date. See commented section below.
				6. Select all commands from "SET NOCOUNT ON" and onwards and excecute the script.
SQL Server: T4025RDP22DB101
SQL Database: QEM Exposure Database
Input Portfolios:	US_HU
Output Tables:
					ABIC_subjFHCF_Full
					ASIC_subjFHCF_Full
					GH_FLxFHCF_Full
					GH_xFL_Full
					GS_FLxFHCF_Full
					GS_xFL_Full
					Clayton_Full
Runtime: < 10 seconds
**********************************************************************************************************************************************/

USE [{{ EDM_FULL_NAME }}]

SET NOCOUNT ON;

DECLARE @Date		VARCHAR(MAX)
DECLARE @portinfoidFull	INT
DECLARE @SQL		VARCHAR(MAX)

SET @portinfoidFull = {{ PORTFOLIO_ID_FULL }} --Update by running (Select * from portinfo). This corresponds to the portfolio you imported that you will now break up into LOBs using this script
SET @Date = {{ DATETIME_VALUE }} --Update by running (Select getdate())


--Lender Placed full
insert into dbo.Portinfo (PORTINFOID,PORTNUM,PORTNAME,CREATEDATE,DESCRIPT)
select max(portinfoid)+1,'USHU_Full_Lender_P','USHU_Full_Lender_P',@Date,'USHU_Full_Lender_P'
 from dbo. Portinfo

DECLARE @PortAcctSeedID_12 INT
SET     @PortAcctSeedID_12 = ( SELECT MAX(ID) FROM seedid where name = 'portacct' )

insert into dbo.Portacct (PORTACCTID,PORTINFOID,ACCGRPID)
select @PortAcctSeedID_12 + ROW_NUMBER() OVER(ORDER BY PORTACCTID),
       (Select max(portinfoid) from portinfo),
          a.accgrpid
		 -- Select count(*)
from   ACCGRP a
inner join portacct p on a.accgrpid = p.accgrpid 
inner join loc l on l.accgrpid = a.ACCGRPID
WHERE a.accgrpid in (select distinct accgrpid from portacct where portinfoid in (@portinfoidfull))
and a.accgrpid in (select distinct accgrpid from accgrp where USERTXT1 like ('%lender%'))

update seedid
set id = (select max (portinfoid) from portacct)
where name = 'portinfo'

update seedid
set id = (select max (portacctid) from portacct)
where name = 'portacct'


--Manufactured Housing full

insert into dbo.Portinfo (PORTINFOID,PORTNUM,PORTNAME,CREATEDATE,DESCRIPT)
select max(portinfoid)+1,'USHU_Full_Manufact','USHU_Full_Manufactured',@Date,'USHU_Full_Manufact'
 from dbo. Portinfo

DECLARE @PortAcctSeedID_13 INT
SET     @PortAcctSeedID_13 = ( SELECT MAX(ID) FROM seedid where name = 'portacct' )

insert into dbo.Portacct (PORTACCTID,PORTINFOID,ACCGRPID)
select @PortAcctSeedID_13 + ROW_NUMBER() OVER(ORDER BY PORTACCTID),
       (Select max(portinfoid) from portinfo),
          a.accgrpid
		  --Select count(*)
from   ACCGRP a
inner join portacct p on a.accgrpid = p.accgrpid 
inner join loc l on l.accgrpid = a.ACCGRPID
WHERE a.accgrpid in (select distinct accgrpid from portacct where portinfoid in (@portinfoidfull))
and a.accgrpid in (select distinct accgrpid from accgrp where USERTXT1 like ('%mobile%'))

update seedid
set id = (select max (portinfoid) from portacct)
where name = 'portinfo'

update seedid
set id = (select max (portacctid) from portacct)
where name = 'portacct'


--Renters full
insert into dbo.Portinfo (PORTINFOID,PORTNUM,PORTNAME,CREATEDATE,DESCRIPT)
select max(portinfoid)+1,'USHU_Full_Renters','USHU_Full_Renters',@Date,'USHU_Full_Renters'
 from dbo. Portinfo

DECLARE @PortAcctSeedID_14 INT
SET     @PortAcctSeedID_14 = ( SELECT MAX(ID) FROM seedid where name = 'portacct' )

insert into dbo.Portacct (PORTACCTID,PORTINFOID,ACCGRPID)
select @PortAcctSeedID_14 + ROW_NUMBER() OVER(ORDER BY PORTACCTID),
       (Select max(portinfoid) from portinfo),
          a.accgrpid
--Select count(*)
from   ACCGRP a
inner join portacct p on a.accgrpid = p.accgrpid 
inner join loc l on l.accgrpid = a.ACCGRPID
WHERE
 a.accgrpid in (select distinct accgrpid from portacct where portinfoid in (@portinfoidfull))
and a.accgrpid in (select distinct accgrpid from accgrp where USERTXT1 like ('%renter%'))

update seedid
set id = (select max (portinfoid) from portacct)
where name = 'portinfo'

update seedid
set id = (select max (portacctid) from portacct)
where name = 'portacct'


--Other full
insert into dbo.Portinfo (PORTINFOID,PORTNUM,PORTNAME,CREATEDATE,DESCRIPT)
select max(portinfoid)+1,'USHU_Full_Other','USHU_Full_Other',@Date,'USHU_Full_Other'
 from dbo. Portinfo

DECLARE @PortAcctSeedID_15 INT
SET     @PortAcctSeedID_15 = ( SELECT MAX(ID) FROM seedid where name = 'portacct' )

insert into dbo.Portacct (PORTACCTID,PORTINFOID,ACCGRPID)
select @PortAcctSeedID_15 + ROW_NUMBER() OVER(ORDER BY PORTACCTID),
       (Select max(portinfoid) from portinfo),
          a.accgrpid
--Select count(*)
from   ACCGRP a
inner join portacct p on a.accgrpid = p.accgrpid 
inner join loc l on l.accgrpid = a.ACCGRPID
WHERE
 a.accgrpid in (select distinct accgrpid from portacct where portinfoid in (@portinfoidfull))
and a.accgrpid in (select distinct accgrpid from accgrp where USERTXT1 like ('%Other%') and USERTXT2 not like ('%clay%'))

update seedid
set id = (select max (portinfoid) from portacct)
where name = 'portinfo'

update seedid
set id = (select max (portacctid) from portacct)
where name = 'portacct'


--Clay 21st full
insert into dbo.Portinfo (PORTINFOID,PORTNUM,PORTNAME,CREATEDATE,DESCRIPT)
select max(portinfoid)+1,'USHU_Full_Clay_21st','USHU_Full_Clay_21st',@Date,'USHU_Full_Clay_21st'
 from dbo. Portinfo

DECLARE @PortAcctSeedID_16 INT
SET     @PortAcctSeedID_16 = ( SELECT MAX(ID) FROM seedid where name = 'portacct' )

insert into dbo.Portacct (PORTACCTID,PORTINFOID,ACCGRPID)
select @PortAcctSeedID_16 + ROW_NUMBER() OVER(ORDER BY PORTACCTID),
       (Select max(portinfoid) from portinfo),
          a.accgrpid
--Select count(*)
from   ACCGRP a
inner join portacct p on a.accgrpid = p.accgrpid 
inner join loc l on l.accgrpid = a.ACCGRPID
inner join policy j on j.ACCGRPID = a.ACCGRPID
WHERE
 a.accgrpid in (select distinct accgrpid from portacct where portinfoid in (@portinfoidfull))
and a.accgrpid in (select distinct accgrpid from accgrp where USERTXT2 like ('%clay%'))
and j.USERIDTXT1 like ('%21st Mort%')

update seedid
set id = (select max (portinfoid) from portacct)
where name = 'portinfo'

update seedid
set id = (select max (portacctid) from portacct)
where name = 'portacct'


--Clay Homes full
insert into dbo.Portinfo (PORTINFOID,PORTNUM,PORTNAME,CREATEDATE,DESCRIPT)
select max(portinfoid)+1,'USHU_Full_Clay_Homes','USHU_Full_Clay_Homes',@Date,'USHU_Full_Clay_Homes'
 from dbo. Portinfo

DECLARE @PortAcctSeedID_17 INT
SET     @PortAcctSeedID_17 = ( SELECT MAX(ID) FROM seedid where name = 'portacct' )

insert into dbo.Portacct (PORTACCTID,PORTINFOID,ACCGRPID)
select @PortAcctSeedID_17 + ROW_NUMBER() OVER(ORDER BY PORTACCTID),
       (Select max(portinfoid) from portinfo),
          a.accgrpid
--Select count(*)
from   ACCGRP a
inner join portacct p on a.accgrpid = p.accgrpid
inner join loc l on l.accgrpid = a.ACCGRPID
inner join policy j on j.ACCGRPID = a.ACCGRPID
WHERE
 a.accgrpid in (select distinct accgrpid from portacct where portinfoid in (@portinfoidfull))
and a.accgrpid in (select distinct accgrpid from accgrp where USERTXT2 like ('%clay%'))
and j.USERIDTXT1 like ('%Homes%')

update seedid
set id = (select max (portinfoid) from portacct)
where name = 'portinfo'

update seedid
set id = (select max (portacctid) from portacct)
where name = 'portacct'

-- ============================================================================
-- RETURN CREATED DATA (Captured by execute_query_from_file)
-- ============================================================================

SELECT
    pi.PORTINFOID,
    pi.PORTNUM,
    pi.PORTNAME,
    pi.CREATEDATE,
    pi.DESCRIPT,
    COUNT(DISTINCT pa.ACCGRPID) AS AccountGroupCount,
    COUNT(DISTINCT pa.PORTACCTID) AS PortfolioAccountCount
FROM dbo.Portinfo pi
LEFT JOIN dbo.Portacct pa ON pi.PORTINFOID = pa.PORTINFOID
WHERE pi.PORTNAME IN (

    'USHU_Full_Lender_P',
    'USHU_Full_Manufact',
    'USHU_Full_Renters',
    'USHU_Full_Other',
    'USHU_Full_Clay_21st',
    'USHU_Full_Clay_Homes'
)
AND pi.CREATEDATE = @Date
GROUP BY pi.PORTINFOID, pi.PORTNUM, pi.PORTNAME, pi.CREATEDATE, pi.DESCRIPT
ORDER BY pi.PORTNAME
