/**********************************************************************************************************************************************
Purpose: This script breaks out the reporting portfolios for US Earthquake exposures in RiskLink
Author: Charlene Chia
Instructions: 
				1. Update quarter e.g. 202209 to 202212. Use Replace All feature.
				2. Update EDM database.
				3. Confirm that there are no changes to the LOBs within User definied fields in the accgrp table. See commented section below.
				4. Update Portinfoid. See commented section below.
				5. Update Date. See commented section below.
				6. Select all commands from "SET NOCOUNT ON" and onwards and excecute the script.
SQL Server: T4025RDP22DB101
SQL Database: QEM Exposure Database
Input Portfolios:	US_EQ
Output Tables:
					US_EQ_Lend
					US_EQ_Prop
					US_EQ_Clayton

Runtime: < 10 seconds
**********************************************************************************************************************************************/

USE [{{ EDM_FULL_NAME }}] --Update

/*US Portfolios:  If these differ, the script will need to be updated
Select distinct userid1 from accgrp
EQ: ASP (Lend+Prop+C

Select * from portinfo -- Use to update "SET" portion below
1	US_EQ

Select distinct USERTXT2 from accgrp -- Used to identify Lend and Prop portfolios
CLAY
PROP
LEND

Select distinct USERTXT1 from accgrp -- Used to identify Clayton. Corresponds to Main_BU in Combined File
Clay
Homeowners
Specialty Property

Select getdate() -- Use to update "SET" portion below
*/

SET NOCOUNT ON;

DECLARE @Date		VARCHAR(MAX)
DECLARE @portinfoid	INT
DECLARE @SQL		VARCHAR(MAX)

SET @portinfoid = {{ PORTFOLIO_ID }}
SET @Date = {{ DATETIME_VALUE }}


--Manufactured Housing

insert into dbo.Portinfo (PORTINFOID,PORTNUM,PORTNAME,CREATEDATE,DESCRIPT)
select max(portinfoid)+1,'USEQ_Manufactured','USEQ_Manufactured',@Date,'USEQ_Manufactured'
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
select max(portinfoid)+1,'USEQ_Renters','USEQ_Renters',@Date,'USEQ_Renters'
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
select max(portinfoid)+1,'USEQ_Condo','USEQ_Condo',@Date,'USEQ_Condo'
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
select max(portinfoid)+1,'USEQ_CHFS','USEQ_CHFS',@Date,'USEQ_CHFS'
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
select max(portinfoid)+1,'USEQ_Other','USEQ_Other',@Date,'USEQ_Other'
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
select max(portinfoid)+1,'USEQ_Clay_21st','USEQ_Clay_21st',@Date,'USEQ_Clay_21st'
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
select max(portinfoid)+1,'USEQ_Clay_Homes','USEQ_Clay_Homes',@Date,'USEQ_Clay_Homes'
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


