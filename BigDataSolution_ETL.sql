USE events_db.dwh;


--Ingest raw data into ingestion table

COPY INTO events_db.dwh.EVENT_INGEST_JSON
FROM
    @EVENTS_DB.DWH.AWS_EVENTS_INGEST
    --files = ( 'event_1.json')
    file_format = (format_name = 'JSON_FILE_FORMAT');

    
--Perform Transform and Load operations

MERGE INTO device d USING (
        SELECT
            RAW_EVENT:DeviceId::VARCHAR as deviceid
        FROM
            EVENT_INGEST_JSON
    ) ei ON d.deviceid = ei.deviceid
    WHEN NOT MATCHED THEN INSERT (DeviceId) VALUES (ei.DeviceId);
    
MERGE INTO dimdatetime d USING (
        select
            RAW_EVENT:Timestamp::TIMESTAMP_NTZ as eventdatetime
        FROM
            EVENT_INGEST_JSON
    ) ei ON d.eventdatetime = ei.eventdatetime
    WHEN NOT MATCHED THEN INSERT (eventdatetime) VALUES (ei.eventdatetime);
    
MERGE INTO dimlocation d USING (
        SELECT
            RAW_EVENT:Location:Country::VARCHAR as Country,
            RAW_EVENT:Location:Region::VARCHAR as Region,
            RAW_EVENT:Location:City::VARCHAR as City
        FROM
            EVENT_INGEST_JSON
    ) ei ON d.Country = ei.Country
    and d.Region = ei.Region
    and d.City = ei.City
    WHEN NOT MATCHED THEN INSERT (Country, Region, City) VALUES (ei.Country, ei.Region, ei.City);
    
INSERT INTO
    EVENTS_DB.DWH.EVENT (DEVICEKEY, LOCATIONKEY, DATETIMEKEY)
SELECT
    DeviceKey,
    locationkey,
    datetimekey
FROM
    EVENTS_DB.DWH.EVENT_INGEST_JSON ei
    JOIN EVENTS_DB.DWH.DEVICE d on ei.RAW_EVENT:DeviceId::VARCHAR = d.deviceid
    join EVENTS_DB.DWH.dimlocation l on ei.RAW_EVENT:Location:Country::VARCHAR = l.country
    and RAW_EVENT:Location:Region::VARCHAR = l.region
    and RAW_EVENT:Location:City::VARCHAR = l.city
    JOIN EVENTS_DB.DWH.dimdatetime ddt on ei.RAW_EVENT:Timestamp::TIMESTAMP_NTZ = ddt.eventyear;
    
MERGE INTO dimpackage d USING (
        SELECT
            DISTINCT value:Name::VARCHAR as Name,
            value:Version::VARCHAR as packageversion,
            value:Type::VARCHAR as type
        FROM
            EVENT_INGEST_JSON,
            table(flatten(RAW_EVENT:Packages))
    ) ei ON d.Name = ei.Name
    and d.packageversion = ei.packageversion
    and d.type = ei.type
    WHEN NOT MATCHED THEN INSERT (Name, packageversion, type) VALUES (ei.Name, ei.packageversion, ei.type);
    
MERGE INTO devicepackagemap d USING (
        SELECT
            RAW_EVENT:DeviceId::VARCHAR as deviceid,
            value:Name::VARCHAR as Name,
            value:Version::VARCHAR as packageversion,
            value:Type::VARCHAR as type
        FROM
            EVENT_INGEST_JSON,
            table(flatten(RAW_EVENT:Packages))
    ) ei ON d.Name = ei.Name
    and d.packageversion = ei.packageversion
    and d.type = ei.type
    WHEN NOT MATCHED THEN INSERT (Name, packageversion, type) VALUES (ei.Name, ei.packageversion, ei.type);

INSERT INTO
    EVENTS_DB.DWH.FACTEVENT
    ...

INSERT INTO
    EVENTS_DB.DWH.DIMSETTINGS
    ...
    
TRUNCATE TABLE events_db.dwh.EVENT_INGEST_JSON;