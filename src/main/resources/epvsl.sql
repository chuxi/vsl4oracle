create tablespace ep datafile 'C:\APP\ADMINISTRATOR\ORADATA\ORCL\EP01.DBF' size 500m autoextend on next 50m segment space management auto;

create temporary tablespace temp_ep tempfile 'C:\APP\ADMINISTRATOR\ORADATA\ORCL\TEMP_EP01.DBF' size 50m;

create user vsl identified by epvsl default tablespace ep temporary tablespace temp_ep;

grant dba to vsl;


drop sequence epvsl_sq;
drop sequence epvslmsg_sq;

drop table epvslmsg;
drop table epvsl;

create table epvsl(
	id number primary key not null,
    vslid varchar2(20) not null,
    vslname varchar2(50) not null,
    vslnamecn varchar2(50),
    expvoy varchar2(10),
    impvoy varchar2(10),
    berpln date,
    unberpln date,
    port varchar2(10),
    berthcode varchar2(10),
    beract date,
    unberact date
);

create table epvslmsg(
	id number primary key not null,
	vsl_id number references epvsl(id),
	content clob
);

create sequence epvsl_sq;
create sequence epvslmsg_sq;

create or replace trigger epvsl_tr
before insert on epvsl
for each row

begin
select epvsl_sq.nextval into :new.id from dual;
end;
/

create or replace trigger epvslmsg_tr
before insert on epvslmsg
for each row

begin
select epvslmsg_sq.nextval into :new.id from dual;
end;
/

