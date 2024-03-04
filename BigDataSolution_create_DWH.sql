CREATE DATABASE Events_db;
USE Events_db;
CREATE SCHEMA dwh;
--CREATE HYBRID TABLE Events_db.dwh.EVENT_INGEST_JSON
CREATE TABLE Events_db.dwh.EVENT_INGEST_JSON (RAW_EVENT VARIANT);
CREATE FILE FORMAT Events_db.dwh.JSON_FILE_FORMAT TYPE = 'JSON' COMPRESSION = 'AUTO' ENABLE_OCTAL = FALSE ALLOW_DUPLICATE = FALSE STRIP_OUTER_ARRAY = TRUE STRIP_NULL_VALUES = FALSE IGNORE_UTF8_ERRORS = FALSE;
CREATE TABLE Events_db.dwh.DimPackage (
    PackageKey INT PRIMARY KEY IDENTITY,
    Name VARCHAR,
    PackageVersion VARCHAR,
    Type VARCHAR
);
CREATE TABLE Events_db.dwh.DimDateTime (
    DateTimeKey INT PRIMARY KEY IDENTITY,
    EventDateTime TIMESTAMP_NTZ;
);
CREATE TABLE Events_db.dwh.DimLocation (
    LocationKey INT PRIMARY KEY IDENTITY,
    Country VARCHAR,
    Region VARCHAR,
    City VARCHAR
);
CREATE TABLE Events_db.dwh.Device (
    DeviceKey INT PRIMARY KEY IDENTITY,
    DeviceId VARCHAR
);
CREATE TABLE Events_db.dwh.DimSetting (
    SettingKey INT PRIMARY KEY IDENTITY,
    PackageKey INT,
    Name VARCHAR,
    Value VARCHAR,
    CONSTRAINT fk_PackageKey FOREIGN KEY (PackageKey) REFERENCES events_db.dwh.DimPackage (PackageKey)
);
CREATE TABLE Events_db.dwh.FactEvent (
    DateTimeKey INT,
    LocationKey INT,
    DeviceCount INT,
    PackageKey INT,
    SettingKey INT,
    SettingCount INT,
    CONSTRAINT fk_DateTimeKey FOREIGN KEY (DateTimeKey) REFERENCES Events_db.dwh.DimDateTime (DateTimeKey),
    CONSTRAINT fk_LocationKey FOREIGN KEY (LocationKey) REFERENCES events_db.dwh.DimLocation (LocationKey),
    CONSTRAINT fk_PackageKey FOREIGN KEY (PackageKey) REFERENCES events_db.dwh.DimPackage (PackageKey),
    CONSTRAINT fk_SettingKey FOREIGN KEY (SettingKey) REFERENCES events_db.dwh.DimSetting (SettingKey)
);
CREATE TABLE Events_db.dwh.Event (
    EventKey INT PRIMARY KEY IDENTITY,
    DeviceKey INT,
    LocationKey INT,
    DatetimeKey INT,
    CONSTRAINT fk_DeviceKey FOREIGN KEY (DeviceKey) REFERENCES events_db.dwh.Device (DeviceKey),
    CONSTRAINT fk_LocationKey FOREIGN KEY (LocationKey) REFERENCES events_db.dwh.DimLocation (LocationKey),
    CONSTRAINT fk_DateTimeKey FOREIGN KEY (DateTimeKey) REFERENCES Events_db.dwh.DimDateTime (DateTimeKey)
);
--CREATE HYBRID TABLE Events_db.dwh.DevicePackageMap
CREATE TABLE Events_db.dwh.DevicePackageMap (
    DeviceKey INT,
    PackageKey INT,
    IsPackegeDeleted BOOLEAN,
    --INDEX idx_packagekey (PackageKey) INCLUDE (IsPackegeDeleted),
    CONSTRAINT fk_DeviceKey FOREIGN KEY (DeviceKey) REFERENCES events_db.dwh.Device (DeviceKey),
    CONSTRAINT fk_PackageKey FOREIGN KEY (PackageKey) REFERENCES events_db.dwh.DimPackage (PackageKey)
);
