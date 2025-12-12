/**********************************************************************************************************************************************
Purpose: This script breaks out the reporting portfolios for US Fire Following Earthquake exposures in RiskLink
Author: Charlene Chia
Instructions: 
				1. Update quarter e.g. 202212 to 202312. Use Replace All feature.
				2. Update EDM database.
				3. Confirm that there are no changes to the LOBs within User definied fields in the accgrp table. See commented section below.
				4. Update Portinfoid. See commented section below.
				5. Update Date. See commented section below.
				6. Select all commands from "SET NOCOUNT ON" and onwards and excecute the script.
SQL Server: T4025RDP22DB101
SQL Database: QEM Exposure Database
Input Portfolios:	US_FF
Output Tables:
					US_FF_Lend
					US_FF_Prop
					US_FF_Clayton

Runtime: < 10 seconds
**********************************************************************************************************************************************/

USE [{{ EDM_FULL_NAME }}] --Update
/*
Select * from portinfo
--1	US_FF

Select distinct userid1 from accgrp
FF: ASP (Lend+Prop+C
Geico HIP 1.0 - FF: 
Geico HIP 2.0 - FF: 

Select distinct USERTXT2 from accgrp
CLAY
LEND
PROP

select getdate()
*/


SET NOCOUNT ON;

DECLARE @Date		VARCHAR(MAX)
DECLARE @portinfoid	INT
DECLARE @SQL		VARCHAR(MAX)

SET @portinfoid = {{ PORTFOLIO_ID }} --Update by running (Select * from portinfo). This corresponds to the portfolio you imported that you will now break up into LOBs using this script
SET @Date = {{ DATETIME_VALUE }} --Update by running (Select getdate())



--Lender Placed
insert into dbo.Portinfo (PORTINFOID,PORTNUM,PORTNAME,CREATEDATE,DESCRIPT)
select max(portinfoid)+1,'USFF_Lender_P','USFF_Lender_P',@Date,'USFF_Lender_P'
 from dbo. Portinfo

DECLARE @PortAcctSeedID_1 INT
SET     @PortAcctSeedID_1 = ( SELECT MAX(ID) FROM seedid where name = 'portacct' )

insert into dbo.Portacct (PORTACCTID,PORTINFOID,ACCGRPID)
select @PortAcctSeedID_1 + ROW_NUMBER() OVER(ORDER BY PORTACCTID),
       (Select max(portinfoid) from portinfo),
          a.accgrpid
		 -- Select count(*)
from   ACCGRP a
inner join portacct p on a.accgrpid = p.accgrpid 
inner join loc l on l.accgrpid = a.ACCGRPID
WHERE a.accgrpid in (select distinct accgrpid from portacct where portinfoid in (@portinfoid))
and a.accgrpid in (select distinct accgrpid from accgrp where USERTXT1 like ('%lender%'))

update seedid
set id = (select max (portinfoid) from portacct)
where name = 'portinfo'

update seedid
set id = (select max (portacctid) from portacct)
where name = 'portacct'

--Geico 1.0
insert into dbo.Portinfo (PORTINFOID,PORTNUM,PORTNAME,CREATEDATE,DESCRIPT)
select max(portinfoid)+1,'USFF_Geico_HIP1','USFF_Geico_HIP1',@Date,'USFF_Geico_HIP1'
 from dbo. Portinfo

DECLARE @PortAcctSeedID_4 INT
SET     @PortAcctSeedID_4 = ( SELECT MAX(ID) FROM seedid where name = 'portacct' )

insert into dbo.Portacct (PORTACCTID,PORTINFOID,ACCGRPID)
select @PortAcctSeedID_4 + ROW_NUMBER() OVER(ORDER BY PORTACCTID),
       (Select max(portinfoid) from portinfo),
          a.accgrpid
		 -- Select count(*)
from   ACCGRP a
inner join portacct p on a.accgrpid = p.accgrpid 
inner join loc l on l.accgrpid = a.ACCGRPID
WHERE a.accgrpid in (select distinct accgrpid from portacct where portinfoid in (@portinfoid))
and a.accgrpid in (select distinct accgrpid from accgrp where USERTXT2 in ('LEND') and USERID1 IN ('Geico HIP 1.0 - FF:'))

update seedid
set id = (select max (portinfoid) from portacct)
where name = 'portinfo'

update seedid
set id = (select max (portacctid) from portacct)
where name = 'portacct'


--Geico 2.0 50QS
insert into dbo.Portinfo (PORTINFOID,PORTNUM,PORTNAME,CREATEDATE,DESCRIPT)
select max(portinfoid)+1,'USFF_Geico_50HIP2','USFF_Geico_50HIP2',@Date,'USFF_Geico_50HIP2'
 from dbo. Portinfo

DECLARE @PortAcctSeedID_5 INT
SET     @PortAcctSeedID_5 = ( SELECT MAX(ID) FROM seedid where name = 'portacct' )

insert into dbo.Portacct (PORTACCTID,PORTINFOID,ACCGRPID)
select @PortAcctSeedID_5 + ROW_NUMBER() OVER(ORDER BY PORTACCTID),
       (Select max(portinfoid) from portinfo),
          a.accgrpid
		 -- Select count(*)
from   ACCGRP a
inner join portacct p on a.accgrpid = p.accgrpid 
inner join loc l on l.accgrpid = a.ACCGRPID
inner join dbo.policy b on b.ACCGRPID = a.ACCGRPID
WHERE a.accgrpid in (select distinct accgrpid from portacct where portinfoid in (@portinfoid))
and a.accgrpid in (select distinct accgrpid from accgrp where USERTXT2 in ('LEND') and USERID1 IN ('Geico HIP 2.0 - FF:'))
and a.accgrpid in (select distinct accgrpid from dbo.policy where inceptdate < '01/01/2024')

update seedid
set id = (select max (portinfoid) from portacct)
where name = 'portinfo'

update seedid
set id = (select max (portacctid) from portacct)
where name = 'portacct'


--Geico 2.0 75QS
insert into dbo.Portinfo (PORTINFOID,PORTNUM,PORTNAME,CREATEDATE,DESCRIPT)
select max(portinfoid)+1,'USFF_Geico_75HIP2','USFF_Geico_75HIP2',@Date,'USFF_Geico_75HIP2'
 from dbo. Portinfo

DECLARE @PortAcctSeedID_6 INT
SET     @PortAcctSeedID_6 = ( SELECT MAX(ID) FROM seedid where name = 'portacct' )

insert into dbo.Portacct (PORTACCTID,PORTINFOID,ACCGRPID)
select @PortAcctSeedID_6 + ROW_NUMBER() OVER(ORDER BY PORTACCTID),
       (Select max(portinfoid) from portinfo),
          a.accgrpid
		 -- Select count(*)
from   ACCGRP a
inner join portacct p on a.accgrpid = p.accgrpid 
inner join loc l on l.accgrpid = a.ACCGRPID
inner join dbo.policy b on b.ACCGRPID = a.ACCGRPID
WHERE a.accgrpid in (select distinct accgrpid from portacct where portinfoid in (@portinfoid))
and a.accgrpid in (select distinct accgrpid from accgrp where USERTXT2 in ('LEND') and USERID1 IN ('Geico HIP 2.0 - FF:'))
and a.accgrpid in (select distinct accgrpid from dbo.policy where inceptdate >= '01/01/2024')

update seedid
set id = (select max (portinfoid) from portacct)
where name = 'portinfo'

update seedid
set id = (select max (portacctid) from portacct)
where name = 'portacct'

--Manufactured Housing

insert into dbo.Portinfo (PORTINFOID,PORTNUM,PORTNAME,CREATEDATE,DESCRIPT)
select max(portinfoid)+1,'USFF_Manufactured','USFF_Manufactured',@Date,'USFF_Manufactured'
 from dbo. Portinfo

DECLARE @PortAcctSeedID_7 INT
SET     @PortAcctSeedID_7 = ( SELECT MAX(ID) FROM seedid where name = 'portacct' )

insert into dbo.Portacct (PORTACCTID,PORTINFOID,ACCGRPID)
select @PortAcctSeedID_7 + ROW_NUMBER() OVER(ORDER BY PORTACCTID),
       (Select max(portinfoid) from portinfo),
          a.accgrpid
		  --Select count(*)
from   ACCGRP a
inner join portacct p on a.accgrpid = p.accgrpid 
inner join loc l on l.accgrpid = a.ACCGRPID
WHERE a.accgrpid in (select distinct accgrpid from portacct where portinfoid in (@portinfoid))
and a.accgrpid in (select distinct accgrpid from accgrp where USERTXT1 like ('%mobile%'))

update seedid
set id = (select max (portinfoid) from portacct)
where name = 'portinfo'

update seedid
set id = (select max (portacctid) from portacct)
where name = 'portacct'


--Renters
insert into dbo.Portinfo (PORTINFOID,PORTNUM,PORTNAME,CREATEDATE,DESCRIPT)
select max(portinfoid)+1,'USFF_Renters','USFF_Renters',@Date,'USFF_Renters'
 from dbo. Portinfo

DECLARE @PortAcctSeedID_8 INT
SET     @PortAcctSeedID_8 = ( SELECT MAX(ID) FROM seedid where name = 'portacct' )

insert into dbo.Portacct (PORTACCTID,PORTINFOID,ACCGRPID)
select @PortAcctSeedID_8 + ROW_NUMBER() OVER(ORDER BY PORTACCTID),
       (Select max(portinfoid) from portinfo),
          a.accgrpid
--Select count(*)
from   ACCGRP a
inner join portacct p on a.accgrpid = p.accgrpid 
inner join loc l on l.accgrpid = a.ACCGRPID
WHERE
 a.accgrpid in (select distinct accgrpid from portacct where portinfoid in (@portinfoid))
and a.accgrpid in (select distinct accgrpid from accgrp where USERTXT1 like ('%renter%'))

update seedid
set id = (select max (portinfoid) from portacct)
where name = 'portinfo'

update seedid
set id = (select max (portacctid) from portacct)
where name = 'portacct'


--Condo
insert into dbo.Portinfo (PORTINFOID,PORTNUM,PORTNAME,CREATEDATE,DESCRIPT)
select max(portinfoid)+1,'USFF_Condo','USFF_Condo',@Date,'USFF_Condo'
 from dbo. Portinfo

DECLARE @PortAcctSeedID_3 INT
SET     @PortAcctSeedID_3 = ( SELECT MAX(ID) FROM seedid where name = 'portacct' )

insert into dbo.Portacct (PORTACCTID,PORTINFOID,ACCGRPID)
select @PortAcctSeedID_3 + ROW_NUMBER() OVER(ORDER BY PORTACCTID),
       (Select max(portinfoid) from portinfo),
          a.accgrpid
--Select count(*)
from   ACCGRP a
inner join portacct p on a.accgrpid = p.accgrpid 
inner join loc l on l.accgrpid = a.ACCGRPID
WHERE
 a.accgrpid in (select distinct accgrpid from portacct where portinfoid in (@portinfoid))
and a.accgrpid in (select distinct accgrpid from accgrp where USERTXT1 like ('%condo%'))

update seedid
set id = (select max (portinfoid) from portacct)
where name = 'portinfo'

update seedid
set id = (select max (portacctid) from portacct)
where name = 'portacct'


--CHFS
insert into dbo.Portinfo (PORTINFOID,PORTNUM,PORTNAME,CREATEDATE,DESCRIPT)
select max(portinfoid)+1,'USFF_CHFS','USFF_CHFS',@Date,'USFF_CHFS'
 from dbo. Portinfo

DECLARE @PortAcctSeedID_2 INT
SET     @PortAcctSeedID_2 = ( SELECT MAX(ID) FROM seedid where name = 'portacct' )

insert into dbo.Portacct (PORTACCTID,PORTINFOID,ACCGRPID)
select @PortAcctSeedID_2 + ROW_NUMBER() OVER(ORDER BY PORTACCTID),
       (Select max(portinfoid) from portinfo),
          a.accgrpid
--Select count(*)
from   ACCGRP a
inner join portacct p on a.accgrpid = p.accgrpid 
inner join loc l on l.accgrpid = a.ACCGRPID
WHERE
 a.accgrpid in (select distinct accgrpid from portacct where portinfoid in (@portinfoid))
and a.accgrpid in (select distinct accgrpid from accgrp where USERTXT1 like ('%Choice%'))

update seedid
set id = (select max (portinfoid) from portacct)
where name = 'portinfo'

update seedid
set id = (select max (portacctid) from portacct)
where name = 'portacct'


--Other
insert into dbo.Portinfo (PORTINFOID,PORTNUM,PORTNAME,CREATEDATE,DESCRIPT)
select max(portinfoid)+1,'USFF_Other','USFF_Other',@Date,'USFF_Other'
 from dbo. Portinfo

DECLARE @PortAcctSeedID_9 INT
SET     @PortAcctSeedID_9 = ( SELECT MAX(ID) FROM seedid where name = 'portacct' )

insert into dbo.Portacct (PORTACCTID,PORTINFOID,ACCGRPID)
select @PortAcctSeedID_9 + ROW_NUMBER() OVER(ORDER BY PORTACCTID),
       (Select max(portinfoid) from portinfo),
          a.accgrpid
--Select count(*)
from   ACCGRP a
inner join portacct p on a.accgrpid = p.accgrpid 
inner join loc l on l.accgrpid = a.ACCGRPID
WHERE
 a.accgrpid in (select distinct accgrpid from portacct where portinfoid in (@portinfoid))
and a.accgrpid in (select distinct accgrpid from accgrp where USERTXT1 like ('%Other%') and USERTXT2 not like ('%clay%'))

update seedid
set id = (select max (portinfoid) from portacct)
where name = 'portinfo'

update seedid
set id = (select max (portacctid) from portacct)
where name = 'portacct'


--Clay 21st
insert into dbo.Portinfo (PORTINFOID,PORTNUM,PORTNAME,CREATEDATE,DESCRIPT)
select max(portinfoid)+1,'USFF_Clay_21st','USFF_Clay_21st',@Date,'USFF_Clay_21st'
 from dbo. Portinfo

DECLARE @PortAcctSeedID_10 INT
SET     @PortAcctSeedID_10 = ( SELECT MAX(ID) FROM seedid where name = 'portacct' )

insert into dbo.Portacct (PORTACCTID,PORTINFOID,ACCGRPID)
select @PortAcctSeedID_10 + ROW_NUMBER() OVER(ORDER BY PORTACCTID),
       (Select max(portinfoid) from portinfo),
          a.accgrpid
--Select count(*)
from   ACCGRP a
inner join portacct p on a.accgrpid = p.accgrpid 
inner join loc l on l.accgrpid = a.ACCGRPID
inner join policy j on j.ACCGRPID = a.ACCGRPID
WHERE
 a.accgrpid in (select distinct accgrpid from portacct where portinfoid in (@portinfoid))
and a.accgrpid in (select distinct accgrpid from accgrp where USERTXT2 like ('%clay%'))
and j.USERIDTXT1 like ('%21st Mort%')

update seedid
set id = (select max (portinfoid) from portacct)
where name = 'portinfo'

update seedid
set id = (select max (portacctid) from portacct)
where name = 'portacct'


--Clay Homes
insert into dbo.Portinfo (PORTINFOID,PORTNUM,PORTNAME,CREATEDATE,DESCRIPT)
select max(portinfoid)+1,'USFF_Clay_Homes','USFF_Clay_Homes',@Date,'USFF_Clay_Homes'
 from dbo. Portinfo

DECLARE @PortAcctSeedID_11 INT
SET     @PortAcctSeedID_11 = ( SELECT MAX(ID) FROM seedid where name = 'portacct' )

insert into dbo.Portacct (PORTACCTID,PORTINFOID,ACCGRPID)
select @PortAcctSeedID_11 + ROW_NUMBER() OVER(ORDER BY PORTACCTID),
       (Select max(portinfoid) from portinfo),
          a.accgrpid
--Select count(*)
from   ACCGRP a
inner join portacct p on a.accgrpid = p.accgrpid
inner join loc l on l.accgrpid = a.ACCGRPID
inner join policy j on j.ACCGRPID = a.ACCGRPID
WHERE
 a.accgrpid in (select distinct accgrpid from portacct where portinfoid in (@portinfoid))
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
    'USFF_Lender_P',
    'USFF_Geico_HIP1',
    'USFF_Geico_50HIP2',
    'USFF_Geico_75HIP2',
    'USFF_Manufactured',
    'USFF_Renters',
    'USFF_Condo',
    'USFF_CHFS',
    'USFF_Other',
    'USFF_Clay_21st',
    'USFF_Clay_Homes'
)
AND pi.CREATEDATE = @Date
GROUP BY pi.PORTINFOID, pi.PORTNUM, pi.PORTNAME, pi.CREATEDATE, pi.DESCRIPT
ORDER BY pi.PORTNAME
