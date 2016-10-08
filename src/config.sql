select top 1 * into sfjl_sync from sfjl;
truncate table sfjl_sync;


alter table sfjl_sync add sync_id varchar(36) primary key not null;

alter table sfjl_sync add sync_date datetime not null;

ALTER TABLE sfjl_sync add sync_type varchar(20);


create trigger sfjl_insert
on sfjl 
    after insert 
as 
insert into sfjl_sync select Yhdm, Lsh, Hh, Kh, Mkey, Fyrq, Dzrq, yjJe, sjJe, Wyj, Syys, Byys, Bz, Sfrq, Sfsj, kpbz,newid(),getdate(),'insert'  from inserted where bz = '1' and Yhdm <> '22'
go


create trigger sfjl_update
on sfjl 
    after update 
as 
declare @bz1 varchar(20)
declare @bz2 varchar(20)
select @bz1 = bz from inserted;
select @bz2 = bz from deleted;
if @bz1 = '1' and @bz2  <> '1'
BEGIN
insert into sfjl_sync select Yhdm, Lsh, Hh, Kh, Mkey, Fyrq, Dzrq, yjJe, sjJe, Wyj, Syys, Byys, Bz, Sfrq, Sfsj, kpbz,newid(),getdate(),'update' from inserted
end
go


