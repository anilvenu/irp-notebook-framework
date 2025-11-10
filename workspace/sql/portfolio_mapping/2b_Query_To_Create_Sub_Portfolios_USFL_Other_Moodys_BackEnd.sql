/**********************************************************************************************************************************************
Purpose: This script breaks out the reporting portfolios for US Other Flood exposures in Risk Modeler
Author: Charlene Chia
Instructions: 
				1. Update quarter e.g. 202212 to 202312. Use Replace All feature.
				2. Update EDM database.
				3. Confirm that there are no changes to the LOBs within User definied fields in the accgrp table. See commented section below.
				4. Update Portinfoid. See commented section below.
				5. Update Date. See commented section below.
				6. Select all commands from "SET NOCOUNT ON" and onwards and excecute the script.

SQL Server: 77aea63098bc8fe4578390278d631a6d.databridge.rms-pe.com
SQL Database: QEM Exposure Database

Runtime: < 10 seconds
**********************************************************************************************************************************************/

USE {{ EDM_FULL_NAME }}

/*--Portfolios:
Select * from portinfo
--3	USFL_Other

Select distinct USERTXT1 from accgrp a
join portacct b on a.ACCGRPID = b.ACCGRPID
where portinfoid = 3
--USERTXT1
--Clayton
--Lender Placed
--Mobile Home
--Other
--Renters

Select distinct USERID1 from accgrp a
join portacct b on a.ACCGRPID = b.ACCGRPID
where portinfoid = 3
-- 'Puerto Rico - FLD -' - using this to pull out CB exposure

Select distinct USERTXT2 from accgrp a
join portacct b on a.ACCGRPID = b.ACCGRPID
where portinfoid = 3
--USERTXT2 - using this to pull out Clayton for the two exposures USFL_Other_Clay_21st & USFL_Other_Clay_Home
--CLAY
--LEND
--PROP

Select distinct USERTXT1,USERTXT2 from accgrp a
join portacct b on a.ACCGRPID = b.ACCGRPID
where portinfoid = 3 order by 1,2
--USERTXT1		USERTXT2
--Clayton		CLAY
--Lender Placed	LEND
--Mobile Home	PROP
--Other			PROP
--Renters		PROP

Select distinct USERIDTXT1 from policy a
join portacct b on a.ACCGRPID = b.ACCGRPID
where portinfoid = 3
--Clay client name there - using this to pull out Clayton for the two exposures USFL_Other_Clay_21st & USFL_Other_Clay_Home
-----------------------------------------------------------------------------------------------------------------------*/

SET NOCOUNT ON;

DECLARE @Date		VARCHAR(MAX)
DECLARE @portinfoid	INT
DECLARE @SQL		VARCHAR(MAX)

SET @portinfoid = {{ PORTFOLIO_ID }} --Update by running (Select * from portinfo). This corresponds to the portfolio you imported that you will now break up into LOBs using this script
SET @Date = {{ DATETIME_VALUE }} --Update by running (Select getdate())

--US Lender Placed
insert into dbo.Portinfo (PORTINFOID,PORTNUM,PORTNAME,CREATEDATE,DESCRIPT)
select max(portinfoid)+1,'USFL_Other_Lender_P','USFL_Other_Lender_P',@Date,'USFL_Other_Lender_P'
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
and a.accgrpid in (select distinct accgrpid from accgrp where USERTXT1 like ('%lender%') and USERID1 not like ('Puerto Rico - FLD -'))

update seedid
set id = (select max (portinfoid) from portacct)
where name = 'portinfo'

update seedid
set id = (select max (portacctid) from portacct)
where name = 'portacct'

--CB Lender Placed
insert into dbo.Portinfo (PORTINFOID,PORTNUM,PORTNAME,CREATEDATE,DESCRIPT)
select max(portinfoid)+1,'USFL_Other_CB','USFL_Other_CB',@Date,'USFL_Other_CB'
 from dbo. Portinfo

DECLARE @PortAcctSeedID_2 INT
SET     @PortAcctSeedID_2 = ( SELECT MAX(ID) FROM seedid where name = 'portacct' )

insert into dbo.Portacct (PORTACCTID,PORTINFOID,ACCGRPID)
select @PortAcctSeedID_2 + ROW_NUMBER() OVER(ORDER BY PORTACCTID),
       (Select max(portinfoid) from portinfo),
          a.accgrpid
		 -- Select count(*)
from   ACCGRP a
inner join portacct p on a.accgrpid = p.accgrpid 
inner join loc l on l.accgrpid = a.ACCGRPID
WHERE a.accgrpid in (select distinct accgrpid from portacct where portinfoid in (@portinfoid))
and a.accgrpid in (select distinct accgrpid from accgrp where USERTXT1 like ('%lender%') and USERID1 like ('Puerto Rico - FLD -'))

update seedid
set id = (select max (portinfoid) from portacct)
where name = 'portinfo'

update seedid
set id = (select max (portacctid) from portacct)
where name = 'portacct'

--Manufactured Housing
insert into dbo.Portinfo (PORTINFOID,PORTNUM,PORTNAME,CREATEDATE,DESCRIPT)
select max(portinfoid)+1,'USFL_Other_Manufactu','USFL_Other_Manufactu',@Date,'USFL_Other_Manufactu'
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
select max(portinfoid)+1,'USFL_Other_Renters','USFL_Other_Renters',@Date,'USFL_Other_Renters'
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

--Other
insert into dbo.Portinfo (PORTINFOID,PORTNUM,PORTNAME,CREATEDATE,DESCRIPT)
select max(portinfoid)+1,'USFL_Other_Other','USFL_Other_Other',@Date,'USFL_Other_Other'
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
select max(portinfoid)+1,'USFL_Other_Clay_21st','USFL_Other_Clay_21st',@Date,'USFL_Other_Clay_21st'
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
select max(portinfoid)+1,'USFL_Other_Clay_Home','USFL_Other_Clay_Home',@Date,'USFL_Other_Clay_Home'
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
    'USFL_Other_Lender_P',
    'USFL_Other_CB',
    'USFL_Other_Manufactu',
    'USFL_Other_Renters',
    'USFL_Other_Other',
    'USFL_Other_Clay_21st',
    'USFL_Other_Clay_Home'
)
AND pi.CREATEDATE = @Date
GROUP BY pi.PORTINFOID, pi.PORTNUM, pi.PORTNAME, pi.CREATEDATE, pi.DESCRIPT
ORDER BY pi.PORTNAME
