

CREATE TABLE fact_occuring_time (
    date_id                  INT,
    year                     INT,
    month_number             INT,
    lsoa_id                  INT,
    lsoa_name                VARCHAR(100),
    location_id              INT,
    location                 VARCHAR(100),
    longitude                DOUBLE PRECISION,
    latitude                 DOUBLE PRECISION,
    crime_type_id            INT,
    crime_type               VARCHAR(100),
    number_of_crime_occuring INT,
    CONSTRAINT pk_fact_occuring_time PRIMARY KEY (date_id, lsoa_id, location_id, crime_type_id),
    CONSTRAINT fk_fot_date
        FOREIGN KEY (date_id) REFERENCES dim_date (date_id),
    CONSTRAINT fk_fot_lsoa
        FOREIGN KEY (lsoa_id) REFERENCES dim_lsoaname (lsoa_id),
    CONSTRAINT fk_fot_location
        FOREIGN KEY (location_id) REFERENCES dim_location (location_id),
    CONSTRAINT fk_fot_crime_type
        FOREIGN KEY (crime_type_id) REFERENCES dim_crime_type (crime_type_id)
);


INSERT INTO fact_occuring_time (
    date_id,
    year,
    month_number,
    lsoa_id,
    lsoa_name,
    location_id,
    location,
    longitude,
    latitude,
    crime_type_id,
    crime_type,
    number_of_crime_occuring
)
SELECT
    -- YYYYMM from TEXT "Date"
    (split_part(cd."Date", '-', 1)::int * 100 +
     split_part(cd."Date", '-', 2)::int)              AS date_id,
    split_part(cd."Date", '-', 1)::int               AS year,
    split_part(cd."Date", '-', 2)::int               AS month_number,
    l.lsoa_id,
    cd."LSOA_name"                                   AS lsoa_name,
    loc.location_id,
    cd."Location"                                    AS location,
    cd."Longitude"                                   AS longitude,
    cd."Latitude"                                    AS latitude,
    ct.crime_type_id,
    cd."Crime_type"                                  AS crime_type,
    COUNT(*)                                         AS number_of_crime_occuring
FROM crime_df cd
JOIN dim_lsoaname   l   ON cd."LSOA_name"  = l.lsoa_name
JOIN dim_location   loc ON cd."Location"   = loc.location
JOIN dim_crime_type ct  ON cd."Crime_type" = ct.crime_type
JOIN dim_date       d   ON d.date_id =
    (split_part(cd."Date", '-', 1)::int * 100 +
     split_part(cd."Date", '-', 2)::int)
GROUP BY
    (split_part(cd."Date", '-', 1)::int * 100 +
     split_part(cd."Date", '-', 2)::int),
    split_part(cd."Date", '-', 1)::int,
    split_part(cd."Date", '-', 2)::int,
    l.lsoa_id,
    cd."LSOA_name",
    loc.location_id,
    cd."Location",
    cd."Longitude",
    cd."Latitude",
    ct.crime_type_id,
    cd."Crime_type";





CREATE TABLE fact_resolution (
    date_id               INT,
    year                  INT,
    month_number          INT,
    lsoa_id               INT,
    lsoa_name             VARCHAR(100),
    location_id           INT,
    location              VARCHAR(100),
    longitude             DOUBLE PRECISION,
    latitude              DOUBLE PRECISION,
    police_officer_strength INT,
    police_staff_strength   INT,
    pcso_strength           INT,
    crime_type_id         INT,
    crime_type            VARCHAR(100),
    outcome_id            INT,
    last_outcome_category VARCHAR(200),
    number_of_resolution  INT,
    CONSTRAINT pk_fact_resolution PRIMARY KEY (date_id, lsoa_id, location_id, crime_type_id, outcome_id),
    CONSTRAINT fk_fact_res_date
        FOREIGN KEY (date_id) REFERENCES dim_date (date_id),
    CONSTRAINT fk_fact_res_lsoa
        FOREIGN KEY (lsoa_id) REFERENCES dim_lsoaname (lsoa_id),
    CONSTRAINT fk_fact_res_location
        FOREIGN KEY (location_id) REFERENCES dim_location (location_id),
    CONSTRAINT fk_fact_res_crime_type
        FOREIGN KEY (crime_type_id) REFERENCES dim_crime_type (crime_type_id),
    CONSTRAINT fk_fact_res_outcome
        FOREIGN KEY (outcome_id) REFERENCES dim_outcome (outcome_id)
);


INSERT INTO fact_resolution (
    date_id,
    year,
    month_number,
    lsoa_id,
    lsoa_name,
    location_id,
    location,
    longitude,
    latitude,
    police_officer_strength,
    police_staff_strength,
    pcso_strength,
    crime_type_id,
    crime_type,
    outcome_id,
    last_outcome_category,
    number_of_resolution
)
SELECT
    -- YYYYMM from TEXT "Date"
    (split_part(cd."Date", '-', 1)::int * 100 +
     split_part(cd."Date", '-', 2)::int)              AS date_id,
    split_part(cd."Date", '-', 1)::int               AS year,
    split_part(cd."Date", '-', 2)::int               AS month_number,
    l.lsoa_id,
    cd."LSOA_name"                                   AS lsoa_name,
    loc.location_id,
    cd."Location"                                    AS location,
    cd."Longitude"                                   AS longitude,
    cd."Latitude"                                    AS latitude,
    cd."Police_Officer_Strength"                     AS police_officer_strength,
    cd."Police_Staff_Strength"                       AS police_staff_strength,
    cd."PCSO_Strength"                               AS pcso_strength,
    ct.crime_type_id,
    cd."Crime_type"                                  AS crime_type,
    o.outcome_id,
    cd."Last_outcome_category"                       AS last_outcome_category,
    COUNT(*)                                         AS number_of_resolution
FROM crime_df cd
JOIN dim_lsoaname   l   ON cd."LSOA_name"             = l.lsoa_name
JOIN dim_location   loc ON cd."Location"              = loc.location
JOIN dim_crime_type ct  ON cd."Crime_type"            = ct.crime_type
JOIN dim_outcome    o   ON cd."Last_outcome_category" = o.last_outcome_category
JOIN dim_date       d   ON d.date_id =
    (split_part(cd."Date", '-', 1)::int * 100 +
     split_part(cd."Date", '-', 2)::int)
GROUP BY
    (split_part(cd."Date", '-', 1)::int * 100 +
     split_part(cd."Date", '-', 2)::int),
    split_part(cd."Date", '-', 1)::int,
    split_part(cd."Date", '-', 2)::int,
    l.lsoa_id,
    cd."LSOA_name",
    loc.location_id,
    cd."Location",
    cd."Longitude",
    cd."Latitude",
    cd."Police_Officer_Strength",
    cd."Police_Staff_Strength",
    cd."PCSO_Strength",
    ct.crime_type_id,
    cd."Crime_type",
    o.outcome_id,
    cd."Last_outcome_category";




CREATE TABLE fact_crime_count (
    date_id         INT,
    year            INT,
    month_number    INT,
    lsoa_id         INT,
    lsoa_name       VARCHAR(100),
    location_id     INT,
    location        VARCHAR(100),
    longitude       DOUBLE PRECISION,
    latitude        DOUBLE PRECISION,
    crime_type_id   INT,
    crime_type      VARCHAR(100),
    number_of_crime INT,
    CONSTRAINT pk_fact_crime_count PRIMARY KEY (date_id, lsoa_id, location_id, crime_type_id),
    CONSTRAINT fk_fact_cc_date
        FOREIGN KEY (date_id) REFERENCES dim_date (date_id),
    CONSTRAINT fk_fact_cc_lsoa
        FOREIGN KEY (lsoa_id) REFERENCES dim_lsoaname (lsoa_id),
    CONSTRAINT fk_fact_cc_location
        FOREIGN KEY (location_id) REFERENCES dim_location (location_id),
    CONSTRAINT fk_fact_cc_crime_type
        FOREIGN KEY (crime_type_id) REFERENCES dim_crime_type (crime_type_id)
);




INSERT INTO fact_crime_count (
    date_id,
    year,
    month_number,
    lsoa_id,
    lsoa_name,
    location_id,
    location,
    longitude,
    latitude,
    crime_type_id,
    crime_type,
    number_of_crime
)
SELECT
    -- Same YYYYMM key from TEXT "Date"
    (split_part(cd."Date", '-', 1)::int * 100 +
     split_part(cd."Date", '-', 2)::int)              AS date_id,
    split_part(cd."Date", '-', 1)::int               AS year,
    split_part(cd."Date", '-', 2)::int               AS month_number,
    l.lsoa_id,
    cd."LSOA_name"                                   AS lsoa_name,
    loc.location_id,
    cd."Location"                                    AS location,
    cd."Longitude"                                   AS longitude,
    cd."Latitude"                                    AS latitude,
    ct.crime_type_id,
    cd."Crime_type"                                  AS crime_type,
    COUNT(*)                                         AS number_of_crime
FROM crime_df cd
JOIN dim_lsoaname   l   ON cd."LSOA_name"  = l.lsoa_name
JOIN dim_location   loc ON cd."Location"   = loc.location
JOIN dim_crime_type ct  ON cd."Crime_type" = ct.crime_type
JOIN dim_date       d   ON d.date_id =
    (split_part(cd."Date", '-', 1)::int * 100 +
     split_part(cd."Date", '-', 2)::int)
GROUP BY
    (split_part(cd."Date", '-', 1)::int * 100 +
     split_part(cd."Date", '-', 2)::int),
    split_part(cd."Date", '-', 1)::int,
    split_part(cd."Date", '-', 2)::int,
    l.lsoa_id,
    cd."LSOA_name",
    loc.location_id,
    cd."Location",
    cd."Longitude",
    cd."Latitude",
    ct.crime_type_id,
    cd."Crime_type";





CREATE TABLE fact_crime_num (
    date_id                 INT,
    year                    INT,
    month_number            INT,
    lsoa_id                 INT,
    lsoa_name               VARCHAR(100),
    location_id             INT,
    location                VARCHAR(100),
    longitude               DOUBLE PRECISION,
    latitude                DOUBLE PRECISION,
    police_officer_strength INT,
    police_staff_strength   INT,
    pcso_strength           INT,
    crime_type_id           INT,
    crime_type              VARCHAR(100),
    number_of_crime         INT,
    CONSTRAINT pk_fact_crime_num PRIMARY KEY (date_id, lsoa_id, location_id, crime_type_id),
    CONSTRAINT fk_fact_crime_num_date
        FOREIGN KEY (date_id) REFERENCES dim_date (date_id),
    CONSTRAINT fk_fact_crime_num_lsoa
        FOREIGN KEY (lsoa_id) REFERENCES dim_lsoaname (lsoa_id),
    CONSTRAINT fk_fact_crime_num_location
        FOREIGN KEY (location_id) REFERENCES dim_location (location_id),
    CONSTRAINT fk_fact_crime_num_crime_type
        FOREIGN KEY (crime_type_id) REFERENCES dim_crime_type (crime_type_id)
);


INSERT INTO fact_crime_num (
    date_id,
    year,
    month_number,
    lsoa_id,
    lsoa_name,
    location_id,
    location,
    longitude,
    latitude,
    police_officer_strength,
    police_staff_strength,
    pcso_strength,
    crime_type_id,
    crime_type,
    number_of_crime
)
SELECT
    -- Build date_id as YYYYMM from TEXT "Date"
    (split_part(cd."Date", '-', 1)::int * 100 +
     split_part(cd."Date", '-', 2)::int)              AS date_id,
    split_part(cd."Date", '-', 1)::int               AS year,
    split_part(cd."Date", '-', 2)::int               AS month_number,
    l.lsoa_id,
    cd."LSOA_name"                                   AS lsoa_name,
    loc.location_id,
    cd."Location"                                    AS location,
    cd."Longitude"                                   AS longitude,
    cd."Latitude"                                    AS latitude,
    cd."Police_Officer_Strength"                     AS police_officer_strength,
    cd."Police_Staff_Strength"                       AS police_staff_strength,
    cd."PCSO_Strength"                               AS pcso_strength,
    ct.crime_type_id,
    cd."Crime_type"                                  AS crime_type,
    COUNT(*)                                         AS number_of_crime
FROM crime_df cd
JOIN dim_lsoaname   l   ON cd."LSOA_name"  = l.lsoa_name
JOIN dim_location   loc ON cd."Location"   = loc.location
JOIN dim_crime_type ct  ON cd."Crime_type" = ct.crime_type
JOIN dim_date       d   ON d.date_id =
    (split_part(cd."Date", '-', 1)::int * 100 +
     split_part(cd."Date", '-', 2)::int)
GROUP BY
    (split_part(cd."Date", '-', 1)::int * 100 +
     split_part(cd."Date", '-', 2)::int),
    split_part(cd."Date", '-', 1)::int,
    split_part(cd."Date", '-', 2)::int,
    l.lsoa_id,
    cd."LSOA_name",
    loc.location_id,
    cd."Location",
    cd."Longitude",
    cd."Latitude",
    cd."Police_Officer_Strength",
    cd."Police_Staff_Strength",
    cd."PCSO_Strength",
    ct.crime_type_id,
    cd."Crime_type";





CREATE TABLE dim_date (
    date_id      INT PRIMARY KEY,   -- e.g. 202201 (YYYYMM)
    year         INT NOT NULL,
    month_number INT NOT NULL,
    year_month   VARCHAR(7) NOT NULL,    -- 'YYYY-MM'
    month_name   VARCHAR(20),
    quarter      INT
);

INSERT INTO dim_date (
    date_id,
    year,
    month_number,
    year_month,
    month_name,
    quarter
)
SELECT
    y * 100 + m                           AS date_id,        -- 2022*100 + 1 = 202201
    y                                     AS year,
    m                                     AS month_number,
    format('%s-%02s', y, m)               AS year_month,     -- '2022-01'
    to_char(make_date(y, m, 1), 'Month')  AS month_name,     -- 'January ', etc.
    extract(quarter FROM make_date(y, m, 1))::int AS quarter
FROM generate_series(2020, 2025) AS y
CROSS JOIN generate_series(1, 12) AS m;



CREATE TABLE dim_outcome (
    outcome_id            INT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    last_outcome_category VARCHAR(200) NOT NULL UNIQUE
);

INSERT INTO dim_outcome (last_outcome_category)
SELECT DISTINCT
    "Last_outcome_category"   -- or adjust to your exact column name
FROM crime_df
WHERE "Last_outcome_category" IS NOT NULL
  AND trim("Last_outcome_category") <> '';


CREATE TABLE dim_location (
    location_id INT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    location    VARCHAR(100) UNIQUE
);

INSERT INTO dim_location (location)
SELECT DISTINCT
    "Location"
FROM crime_df
WHERE "Location" IS NOT NULL AND trim("Location") <> '';


CREATE TABLE dim_lsoaname (
    lsoa_id   INT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    lsoa_name VARCHAR(100) NOT NULL UNIQUE,
    lsoa_code VARCHAR(50)
);

INSERT INTO dim_lsoaname (lsoa_name, lsoa_code)
SELECT DISTINCT
    "LSOA_name",
    "LSOA_code"
FROM crime_df
WHERE "LSOA_name" IS NOT NULL;



CREATE TABLE dim_crime_type (
    crime_type_id INT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    crime_type    VARCHAR(100) NOT NULL UNIQUE
);

INSERT INTO dim_crime_type (crime_type)
SELECT DISTINCT "Crime_type"
FROM crime_df
WHERE "Crime_type" IS NOT NULL;



SELECT * from crime_df LIMIT 5

SELECT * from dim_date LIMIT 5