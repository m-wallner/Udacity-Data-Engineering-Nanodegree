# Collection of all SQL queries used for the project

# Airports
drop_airports = "DROP TABLE IF EXISTS airports;"

create_airports = """
CREATE TABLE IF NOT EXISTS public.airports (
    iata_code    VARCHAR PRIMARY KEY,
    name         VARCHAR,
    type         VARCHAR,
    local_code   VARCHAR,
    coordinates  VARCHAR,
    city         VARCHAR,
    elevation_ft FLOAT,
    continent    VARCHAR,
    iso_country  VARCHAR,
    iso_region   VARCHAR,
    municipality VARCHAR,
    gps_code     VARCHAR);"""

airport_insert = """
    INSERT INTO airports (
        iata_code, name, type, local_code, coordinates, city, elevation_ft,
        continent, iso_country, iso_region, municipality, gps_code)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);"""


# Demographics
drop_demographics = "DROP TABLE IF EXISTS demographics;"

create_demographics = """
    CREATE TABLE IF NOT EXISTS public.demographics (
        city                   VARCHAR,
        state                  VARCHAR,
        media_age              FLOAT,
        male_population        INT,
        female_population      INT,
        total_population       INT,
        num_veterans           INT,
        foreign_born           INT,
        average_household_size FLOAT,
        state_code             VARCHAR,
        race                   VARCHAR,
        count                  INT);
"""

demographics_insert = """
    INSERT INTO demographics (
        city, state, media_age, male_population, female_population, total_population,
        num_veterans, foreign_born, average_household_size, state_code, race, count)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
"""


# Immigration
drop_immigration = "DROP TABLE IF EXISTS immigration;"

create_immigration = """
    CREATE TABLE IF NOT EXISTS public.immigration (
        cicid    FLOAT PRIMARY KEY,
        year     FLOAT,
        month    FLOAT,
        cit      FLOAT,
        res      FLOAT,
        iata     VARCHAR,
        arrdate  FLOAT,
        mode     FLOAT,
        addr     VARCHAR,
        depdate  FLOAT,
        bir      FLOAT,
        visa     FLOAT,
        count    FLOAT,
        dtadfile VARCHAR,
        entdepa  VARCHAR,
        entdepd  VARCHAR,
        matflag  VARCHAR,
        biryear  FLOAT,
        dtaddto  VARCHAR,
        gender   VARCHAR,
        airline  VARCHAR,
        admnum   FLOAT,
        fltno    VARCHAR,
        visatype VARCHAR);
"""

immigration_insert = """
    INSERT INTO immigration (cicid, year, month, cit, res, iata, arrdate, mode, addr, depdate, bir, visa,
        count, dtadfile, entdepa, entdepd, matflag, biryear, dtaddto, gender, airline, admnum, fltno, visatype)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
"""


# Temperature
drop_temperature = "DROP TABLE IF EXISTS temperature;"

create_temperature = """
    CREATE TABLE IF NOT EXISTS temperature (
        timestamp                       DATE,
        avg_temperature                 FLOAT,
        avg_temperature_uncertainty     FLOAT,
        city                            VARCHAR,
        country                         VARCHAR,
        latitude                        VARCHAR,
        longitude                       VARCHAR);
"""

temperature_insert = """
    INSERT INTO temperature (timestamp, avg_temperature, avg_temperature_uncertainty,
        city, country, latitude, longitude)
    VALUES (%s, %s, %s, %s, %s, %s, %s)
"""


# Combine SQL queries into lists
drop_table_queries = [drop_airports, drop_demographics, drop_immigration, drop_temperature]
create_table_queries = [create_airports, create_demographics, create_immigration, create_temperature]