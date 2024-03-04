--Possibility to retrieve device list by value of a particular package setting at some point in time

create view as
	select
		dp.name
		, pd.packageversion
		, dp.type
		, ddt.eventdatetime
		, ds.name
		, ds.value
		, d.deviceid
	from device d
	join event e on d.devicekey = e.devicekey
	join dimdatetime ddt on e.datetimekey = ddt.datetimekey
	join devicepackagemap dpm on d.devicekey = dpm.devicekey
	join dimpackage dp on dpm.packagekey = dp.packagekey
	join dimsetting ds on dp.packagekey = ds.packagekey;


--How many devices have installed specified packages by hours/days/weeks? 

create view as
	select
		dp.name
		, pd.packageversion
		, dp.type
		, sum(fe.devicecount) as devicecount
		, ddt.eventdatetime
	from factevent fe
	join dimpackage dp on fe.packagekey = dp.packagekey
	join dimdatetime ddt on fe.datetimekey = ddt.datetimekey
	where cast(ddt.datetime as year) > 2023
	group by dp.name, pd.packageversion, dp.type, ddt.eventdatetime



--Devices distribution by country/city/region

create view as
	select
		dl.country
		, dl.city
		, dl.region
		, sum(devicecount) as devicecount
	from factevent fe
	join dimlocation dl on fe.locationkey = dl.locationkey
	group by dl.country, dl.city, dl.region;



--How does the versions distribution for a particular package change over time?

create view as 
	select
		dp.name
		, dp.packageversion
		, dp.type
		, sum(packagecount) as packagecount
		, ddt.eventdatetime
	from factevent fe
	join dimpackage dp on fe.packagekey = dp.packagekey
	join dimdatetime ddt on fe.datetimekey = ddt.datetimekey
	group by dp.name, dp.packageversion, dp.type;




--How does the value distribution for a particular package setting change over time by package version?

create view as 
	select
		dp.name
		, dp.packageversion
		, dp.type
		, ds.settingkey
		, sum(fe.settingcount) as settingcount
		, ddt.eventdatetime
	from factevent fe
	join dimdatetime ddt on fe.datetimekey = ddt.datetimekey
	join dimpackage dp on fe.packagekey = dp.packagekey
	join dimsetting ds on dp.packagekey = ds.packagekey
	group by dp.name, dp.packageversion, dp.type, ds.settingkey, , ddt.eventdatetime;
