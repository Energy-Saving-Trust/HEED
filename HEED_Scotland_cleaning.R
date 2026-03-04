#Update Overview:

#1. Load and prepare raw datasets
#2. Join property attribute codes to names, cleanse categories and write out updated datasets
#   -Property type
#   -Property tenure
#   -Property age
#   -Loft insulation
#   -Wall construction
#   -Main fuel type
#   -Main heating system
#   -Glazing type

###################################################################################################################
library(RODBC)
library(tools)
library(readxl)
library(tidyverse)
library(data.table)

#Set options so that R does not report using scientific notation
options(scipen=999)

# TODO: What is this?
#ODBC connection:
#In order to connect to databases other than 'EST_Select_Process' the appropriate User DSN must be added
#in ODBC Data Source Administrator. To do this, copy the configuration settings used by the 'EST_Select_Process'.

#Driver:            SQL Server Native Client 10.0
#Name:              [Insert database name]
#Description:       [Insert database name]
#Server:            10.70.198.10
#Login ID:          select_odbc
#Password:          $elect2015
#Default database:  [Insert database name]

#Set path of working directory
# TODO: Config files might be a shout to control some options below too
path = "J:/HA GOL/"
local_path = "C:/Users/david.grady/Local files/HEED/"

# Point to the correct files and sheets in files
eesabs_file = "ABS MASTER ALL DATA Sept2025.xlsx"
# TODO: Talk to Christiana about getting one file that stays the same in terms of formatting 
sheet = "ABS MASTER ALL DATA Feb2026"
whs_file = "WHS installation data_live.xlsx"

# TODO: Add in lists of columns we expect to get from each DB so we can do a check if there are any new columns or renamed columns
# TODO: Log changes, which vars get mapped to what and how many records we get for each group etc

###################################################################################################################
#1. Load and clean raw data (HEEPS ABS, WHS, existing HEED records)
#HEEPS ABS Data

# In the future, we will pull all HEEPSABS data fromm the HEEPSABS database but currently Kaisa hasn't had a chance to pick this up from Andreas, so there is no
# table in the database with the new records. Instead we will read in a CSV that has all the new HEEPSABS records installed since the last update (April 2018)
# NOTE: DG. That future is coming. Next update (Summer 2025 will have the full dataset at once.)

# Connect to database and load in data:
# TODO: Do we want old PCONs or new PCONs? Can we get the coordinates in future since we can just join
connect = odbcConnect('EST_HEEPSABS_30_Presentation')
# Changed to "AS" aliasing because using the old "=" isnt SQL safe I dont think(?) so isnt the best practice
query = paste("SELECT
              nspd.PCON_CODE AS OLD_PCON_CODE,
              '' AS PCON_CODE,
              heeps.UPRN AS UPRN,
              heeps.Completion_Date AS TIMESTAMP,
              heeps.Tenure AS TENURE,
              heeps.Measure_Name AS MEASURE,
              heeps.NonECO_Funded_Measure AS Non_ECO_Measure,
              heeps.Post_Code AS POSTCODE

              FROM [EST_HEEPSABS_30_Presentation].[dbo].[ALL_HEEPSABS_MEASURES_30042018] heeps
              LEFT OUTER JOIN [EST_HEED_30_Presentation].[dbo].[NSPD_LOOKUP] nspd
              on heeps.Post_Code COLLATE DATABASE_DEFAULT = nspd.POSTCODE ;")
heeps = sqlQuery(connect, query)

# Close the ODBC connection when you're done
odbcClose(connect)

heeps$POSTCODE <- toupper(as.character(heeps$POSTCODE))
heeps$POSTCODE <- gsub(" |\\?|\"", "", heeps$POSTCODE)
# TODO: Convert the UPRN as string for now because the newer ABS data is a CSV so comes through as CHAR
heeps$UPRN <- as.character(heeps$UPRN)

#HEEPS 2018 to current (incl blanks and unknowns for certainty)
# heeps_new = read.csv(file = paste0(path, "EES ABS/Post_Andreas_ABS_Data.csv"), encoding = "latin1")
# NOTE: Now we just get from Christiana in the EES:ABS role in the HES data team rather than prepping
# The CSV with only new records
# TODO: Fix the timestamps so we can just load in with Excel guessing the types properly 
# It is kinda nice with dates since it NAs the issues out but we do have some cleaning scripts to try and grab some dates
heeps_new = read_excel(paste0(path, "EES ABS/", eesabs_file), col_types = "text", sheet = sheet)
names(heeps_new) <- gsub("\\.+", " ", names(heeps_new))
names(heeps_new) <- gsub("\\s+", " ", names(heeps_new))
names(heeps_new) <- trimws(names(heeps_new))

table(heeps_new$FYEAR)

# Lets scrape what we can from the FYEAR
# Get the first year in the string and assume Jan 01
heeps_new <- heeps_new %>%
  mutate(
    FYEAR_YEAR = str_extract(FYEAR, "\\d{4}"),
    FYEAR_DATE = if_else(is.na(FYEAR_YEAR), NA_Date_, as.Date(paste0("01/01/", FYEAR_YEAR), format = "%d/%m/%Y"))
  )

# Change key fields to be all lower case to help avoid double counting things like
# EXTERNAL WALL INSULATION and external wall insulation -- these are 2 unique entries for the same thing
heeps_new <- heeps_new %>%
  rename(
    MEASURE = `Measure as provided in quarterly data submission`,
    Non_ECO_Measure = `If Non-ECO Funded Measure - Please Specify`,
    TIMESTAMP = `Completion Date (of measure)`,
    POSTCODE = PCODE
  )

# Bring the old and new(er) together
heeps_bind <- heeps %>%
  bind_rows(heeps_new) %>%
  mutate(across(c(MEASURE, Non_ECO_Measure), tolower),
         mutate(across(where(is.character), str_squish))) %>%
  # If both measure columns are blank then not much we can do 
  filter(!(
    (is.na(MEASURE) | MEASURE %in% c("", " ")) &
      (is.na(Non_ECO_Measure) | Non_ECO_Measure %in% c("", " "))
  )
  ) %>%
  # Replace MEASURE in some cases to fill gaps
  mutate(
    MEASURE = case_when(
      MEASURE %in% c("non-ecofundedmeasure", "non-eco funded measure") |
        (MEASURE %in% c("", " ") | is.na(MEASURE)) & !(Non_ECO_Measure %in% c("", " ") | is.na(Non_ECO_Measure)) ~ Non_ECO_Measure,
      TRUE ~ MEASURE
    ),
    # No codes for now and we'll need to get this in this script, but future work may be able to provide as standard
    PCON_CODE = NA_character_,
    UPRN = as.numeric(UPRN)
  ) %>%
  select(UPRN, TENURE, PCON_CODE, TIMESTAMP, MEASURE, POSTCODE, FYEAR_DATE)

#Remove missing values
# TODO: ADDRESS MATCH THESE?!
heeps_bind <- heeps_bind %>%
  filter(!is.na(UPRN)) %>%
  # Some in the old EES ABS data with no measures?
  filter(!is.na(MEASURE))

#Load the latest ONS UPRN data
# TODO set the file more dynamically for future updates
# TODO why do we do this if we dont for WHS or update the PCON for the HEED data in the server?
# Peek at the column names by reading just the first row
onsud = "J:/Central resources/Lookups/ONSUD_NOV_2024_SC.csv"
onspd = "J:/Central resources/Lookups/ONSPD_NOV_2024_UK.csv"

cols_ud <- fread(onsud, nrows = 1)
cols_pd <- fread(onspd, nrows = 1)

# View the column names
colnames(cols_ud)
colnames(cols_pd)

if(!exists("ons_uprn")){
  print("Loading in ONS UPRN data")
  ons_uprn = fread(file=onsud
                   , select = c("UPRN", "PCON24CD")
                   # Set as double because SENSIBLY this will convert to int64 otherwise. 
                   # Double is what we use which is mad.
                   , colClasses = c("UPRN" = "double")
  )
}

# TODO: Use ONSPD for grabbing any extra codes?
# pd = fread(file = onspd
#            , select = c("pcds", "pcon")
#              )

# Bring in ONS data for PCON_CODE
heeps <- heeps_bind %>%
  mutate(across(c(MEASURE, TENURE), tolower),
         mutate(across(where(is.character), str_squish))) %>%
  left_join(ons_uprn, by = "UPRN") %>%
  mutate(PCON_CODE = PCON24CD) %>%
  select(!PCON24CD) %>%
  # Remove any without a PCON CODE
  filter(!is.na(PCON_CODE))

#Convert Date field
heeps$TIMESTAMP = as.character(heeps$TIMESTAMP)

#Identify all cases that don't have a proper date record
ind = which(substr(heeps$TIMESTAMP, 3,3) != "/" | is.na(heeps$TIMESTAMP)==TRUE | heeps$TIMESTAMP=="") #22,140
table(heeps[ind,c("TIMESTAMP")])

#Identify cases where date is stored in excel numeric format (e.g. 39641)
# Filter and modify the rows with corrupted dates directly
heeps <- heeps %>%
  mutate(
    TIMESTAMP = if_else(
      # TODO Add in other dates 
      substr(TIMESTAMP, 1, 2) %in% c("39", "43"), 
      # We can convert the numbers to dates leveraging when we know Excel starts encoding dates to numbers.
      # It may not be totally accurate but it's something
      # Use 1899 so we get around the leap year issues in Excel
      as.character(format(as.Date(as.numeric(TIMESTAMP) - 2, origin = "1899-12-30"), "%d/%m/%Y")), 
      TIMESTAMP
    )
  )

# Clean the date column
# TODO: Can we cool it on the if_else statements? Just do a single case_when()
heeps <- heeps %>%
  mutate(
    TIMESTAMP = gsub("\\.", "/", TIMESTAMP),
    TIMESTAMP = str_trim(TIMESTAMP), 
    # Add leading zero to day if single-digit
    TIMESTAMP = if_else(substr(TIMESTAMP, 1, 2) %in% c("1/", "2/", "3/", "4/", "5/", "6/", "7/", "8/", "9/"),
                        paste0("0", TIMESTAMP), TIMESTAMP),
    # Add leading zero to month if single-digit
    TIMESTAMP = if_else(substr(TIMESTAMP, 3, 5) %in% c("/1/", "/2/", "/3/", "/4/", "/5/", "/6/", "/7/", "/8/", "/9/"),
                        paste0(substr(TIMESTAMP, 1, 3), "0", substr(TIMESTAMP, 4, 9)), TIMESTAMP),
    # Handle some edge cases where we can determine a better date than 01/01/2015
    # TODO: Find a better way to automate this. Maybe we can switch the 2nd and 3rd digits if above current
    # year +1 and then if that doesnt work set to a set year like 2015
    TIMESTAMP = case_when(
      TIMESTAMP == "Aug-20" ~ "01/08/2020",
      TIMESTAMP == "Sep-20" ~ "01/09/2020",
      TIMESTAMP == "15/11/017" ~ "15/11/2017",
      TIMESTAMP == "14/08/209" ~ "14/08/2019",
      TIMESTAMP == "July/Aug 2020" ~ "01/07/2020",
      TIMESTAMP == "Wk com 25/10/2021" ~ "25/10/2021",
      TIMESTAMP == "Wk com 18/10/2021" ~ "18/10/2020",
      TIMESTAMP == "30/09/2106" ~ "30/09/2016",
      TRUE ~ TIMESTAMP
    ),
    # Convert to a date
    TIMESTAMP = if_else(grepl("^\\d{2}/\\d{2}/\\d{4}$", TIMESTAMP), 
                        as.Date(TIMESTAMP, "%d/%m/%Y"), NA),
    TIMESTAMP = if_else(is.na(TIMESTAMP), FYEAR_DATE, TIMESTAMP),
    # Grab the year from the date for checking
    YEAR = format(TIMESTAMP, "%Y"),
    TIMESTAMP = if_else(as.numeric(YEAR) < 2010 | 
                          is.na(TIMESTAMP), 
                        as.Date("01/01/2015", "%d/%m/%Y"), TIMESTAMP)
  )

table(heeps$YEAR, useNA = "ifany")

#Clean measure category
#Create measure indicies
#Check to see if there are any additional measures that need to be accounted for
# TODO: Make this list a lookup so we can log if there are any new measures
sort(unique(heeps$MEASURE))

heeps <- heeps %>%
  mutate(
    SOURCE = "HEEPS ABS",
    HEEPS_TENURE = case_when(
      grepl("owner|private - o", TENURE) ~ "Owner Occupier",
      grepl("private", TENURE) ~ "Privately Rented",
      grepl("social|local authority", TENURE) ~ "Social Housing",
      grepl("housing association|rsl|ha rent", TENURE) ~ "Rented from Housing Association",
      TRUE ~ "Unknown Tenure"
    ),
    HEEPS_LOFT_INS = case_when(
      # TODO: Why do we assume these?
      # In the original code we assume "loft insulation" is know thickness but 
      # "Loft Insulation" is unknown?
      MEASURE == "loft insulation" ~ "Loft Insulation (Unknown Thickness)",
      grepl("virgin", MEASURE) ~ "Loft Insulation 0 - 270mm",
      grepl("loft insulation", MEASURE) &
        grepl("top up|rafter", MEASURE) ~ "Loft Insulation 100-300mm",
      TRUE ~ "Unknown"
    ),
    HEEPS_WALL_INS = case_when(
      # TODO better automate this
      MEASURE == "external wall insulation for cavity walls" ~ "Cavity Wall EWI",
      grepl("timber frame", MEASURE) &
        grepl("internal\\s?wall|iwi", MEASURE) ~ "Timber Frame IWI",
      grepl("cavity", MEASURE) & 
        grepl("internal\\s?wall|iwi", MEASURE) ~ "Cavity Wall IWI",
      grepl("cavity|cwi", MEASURE) ~ "Cavity Wall Filled",
      grepl("external\\s?wall|ewi", MEASURE) ~ "Solid Wall - Externally Insulated",
      # Sometime has no spaces??
      grepl("internal\\s?wall|iwi", MEASURE) ~ "Solid Wall - Internally Insulated",
      MEASURE == "wall insulation" ~ "Wall Insulation (Unknown Wall Type)",
      TRUE ~ "Unknown"
    ),
    HEEPS_PROP_TYPE = "Unknown",
    HEEPS_PROP_AGE = "Unknown",
    HEEPS_GLAZ_TYPE = case_when(
      grepl("window glazing|double glazed window", MEASURE) ~ "Full Double Glazing",
      TRUE ~ "Unknown"
    ),
    HEEPS_FUEL_TYPE = case_when(
      grepl("heat pump|electric heating|storage heat", MEASURE) ~ "Electricity",
      grepl("gas heating system", MEASURE) ~ "Gas",
      grepl("biomass", MEASURE) ~ "Biomass",
      grepl("oil heating|oil boiler", MEASURE) ~ "Oil",
      grepl("solid fuel", MEASURE) ~ "Solid Fuel",
      TRUE ~ "Unknown"
    ),
    HEEPS_FUEL_SYSTEM = case_when(
      grepl("boiler", MEASURE) ~ "Condensing Regular Boiler",
      grepl("district", MEASURE) ~ "Community",
      grepl("electric heating|storage heat", MEASURE) ~ "Electric Storage Heaters",
      grepl("heat pump", MEASURE) ~ "Heat Pump",
      TRUE ~ "Unknown"
    ),
    HEEPS_RENEWABLES = case_when(
      grepl("air source heat pump", MEASURE) ~ "ASHP",
      grepl("photovoltaics", MEASURE) ~ "SOLAR_PV",
      grepl("battery storage", MEASURE) ~ "BATTERY",
      # TODO: What about others?
      TRUE ~ "Unknown"
    ),
    HEEPS_DRAUGHT = case_when(
      grepl("draught", MEASURE) ~ "Draught Proofing (General)",
      TRUE ~ "Unknown"
    ),
    HEEPS_PIPEWORK_INS = case_when(
      grepl("pipework", MEASURE) ~ "Primary Pipework Insulation",
      TRUE ~ "Unknown"
    ),
    HEEPS_RIR_INS = case_when(
      MEASURE == "room in roof insulation" ~ "Room in Roof Insulation",
      TRUE ~ "Unknown"
    ),
    HEEPS_FLAT_ROOF_INS = case_when(
      MEASURE == "flat roof insulation" ~ "Flat Roof Insulation",
      TRUE ~ "Unknown"
    ),
    HEEPS_FLOOR_INS = case_when(
      grepl("under\\s?floor", MEASURE) ~ "Suspended Floor Insulation",
      grepl("floor insulation", MEASURE) ~ "Floor Insulation",
      TRUE ~ "Unknown"
    )
  )

final_heeps <- heeps %>%
  select(UPRN, PCON_CODE, TIMESTAMP, YEAR, SOURCE,
         HEEPS_PROP_TYPE, HEEPS_TENURE, HEEPS_LOFT_INS, 
         HEEPS_PROP_AGE, HEEPS_WALL_INS, HEEPS_GLAZ_TYPE, 
         HEEPS_FUEL_TYPE, HEEPS_FUEL_SYSTEM, HEEPS_FLOOR_INS,
         HEEPS_FLAT_ROOF_INS, HEEPS_RIR_INS, HEEPS_RENEWABLES)

####
# Warmer Homes Scotland (WHS) Data
## Switching to reading the WHS data in as excel sheet to prevent the UPRNs from changing to scientific notation
whs_raw <- read_excel(paste0(path, "WHS/LIVE/", whs_file), guess_max = 10000)
names(whs_raw) <- gsub("\\.+", " ", names(whs_raw))
names(whs_raw) <- gsub("\\s+", " ", names(whs_raw))
names(whs_raw) <- trimws(names(whs_raw))

whs <- whs_raw %>%
  filter(!(UPRN %in% c("", " ", "#N/A", "NA") | is.na(UPRN))) %>%
  mutate(
    UPRN = as.numeric(UPRN),
    TIMESTAMP = as.character(`Master Completion Date`),
    TIMESTAMP = gsub("\\.", "/", TIMESTAMP),
    TIMESTAMP = str_trim(TIMESTAMP), 
    # Add leading zero to day if single-digit
    TIMESTAMP = if_else(substr(TIMESTAMP, 1, 2) %in% c("1/", "2/", "3/", "4/", "5/", "6/", "7/", "8/", "9/"),
                        paste0("0", TIMESTAMP), TIMESTAMP),
    # Add leading zero to month if single-digit
    TIMESTAMP = if_else(substr(TIMESTAMP, 3, 5) %in% c("/1/", "/2/", "/3/", "/4/", "/5/", "/6/", "/7/", "/8/", "/9/"),
                        paste0(substr(TIMESTAMP, 1, 3), "0", substr(TIMESTAMP, 4, 9)), TIMESTAMP),
    # Handle some edge cases where we can determine a better date than 01/01/2015
    # TODO: Find a better way to automate this. Maybe we can switch the 2nd and 3rd digits if above current
    # year +1 and then if that doesnt work set to a set year like 2015
    TIMESTAMP = case_when(
      TIMESTAMP == "Aug-20" ~ "01/08/2020",
      TIMESTAMP == "Sep-20" ~ "01/09/2020",
      TIMESTAMP == "15/11/017" ~ "15/11/2017",
      TIMESTAMP == "14/08/209" ~ "14/08/2019",
      TIMESTAMP == "July/Aug 2020" ~ "01/07/2020",
      TIMESTAMP == "Wk com 25/10/2021" ~ "25/10/2021",
      TIMESTAMP == "Wk com 18/10/2021" ~ "18/10/2020",
      TIMESTAMP == "30/09/2106" ~ "30/09/2016",
      TRUE ~ TIMESTAMP
    ),
    # Convert to a date
    TIMESTAMP = if_else(grepl("^\\d{4}-\\d{2}-\\d{2}$", TIMESTAMP), 
                        as.Date(TIMESTAMP, "%Y-%m-%d"), as.Date("2015/01/01", "%Y/%m/%d")),
    # Grab the year from the date for checking
    YEAR = format(TIMESTAMP, "%Y"),
    TIMESTAMP = if_else(as.numeric(YEAR) < 2010, as.Date("2015/01/01", "%Y/%m/%d"), TIMESTAMP)
  )

# Map out the measures, fuel types etc
whs <- whs %>%
  # Lets get everything into a consistent case for simplicity
  mutate(across(where(is.character), ~ str_squish(tolower(.)))) %>%
  mutate(
    SOURCE = "Warmer Homes Scotland",
    WHS_PROP_TYPE = "Unknown",
    WHS_TENURE = case_when(
      grepl("owner", Tenure) ~ "Owner Occupier",
      grepl("private", Tenure) ~ "Privately Rented",
      grepl("local authority", Tenure) ~ "Social Housing",
      TRUE ~ "Unknown"
    ),
    # TODO: Can we really not make an assumption when WHS says "loft insulation" -- check with the team on what is required
    # Maybe WHS only offers 250mm+?
    WHS_LOFT_INS = case_when(
      `Loft Insulation` == "yes" |
        `Pitched Roof Insulation` == "yes" ~ "Loft Insulation 100-300mm",
      TRUE ~ "Unknown"
    ),
    WHS_FLAT_ROOF_INS = case_when(
      `Flat Roof Insulation` == "yes" ~ "Flat Roof Insulation",
      TRUE ~ "Unknown"
    ),
    WHS_PROP_AGE = "Unknown",
    WHS_WALL_INS = case_when(
      `Cavity Wall Insulation` == "yes" ~ "Cavity Wall Filled",
      `External Wall Insulation` == "yes" ~ "Solid Wall - Externally Insulated",
      `Internal Wall Insulation` == "yes" | 
        `Injected Internal Wall Insulation` == "yes" ~ "Solid Wall - Internally Insulated",
      `Hybrid Wall Insulation` == "yes" ~ "Wall Insulation (Unknown Wall Type)",
      TRUE ~ "Unknown"
    ),
    WHS_GLAZ_TYPE = case_when(
      # TODO: Can we check with the WHS team on if these are more likely to be windows than doors?
      `Energy Efficient glazing/doors` == "yes" ~ "Full Double Glazing",
      TRUE ~ "Unknown"
    ),
    WHS_FUEL_TYPE = case_when(
      `Biomass Boiler` == "yes" | 
        `Biomass Boiler Back Boiler` == "yes" ~ "Biomass",
      # TODO: Include `Flue Gas Recovery Device`?
      `Gas Connection` == "yes" |
        `Gas Fired Condensing Boilers` == "yes" ~ "Gas",
      `Gas Boiler LPG` == "yes" ~ "LPG",
      `Oil Fired Condensing Boilers` == "yes" | 
        `Oil Tank` == "yes" ~ "Oil",
      `Electric Storage Heaters` == "yes" | 
        `Air Source Heat Pump` == "yes" ~ "Electricity",
      TRUE ~ "Unknown"
    ),
    WHS_FUEL_SYSTEM = case_when(
      `Air Source Heat Pump` == "yes" ~ "Heat Pump",
      `Electric Storage Heaters` == "yes" ~ "Electric Storage Heaters",
      `Gas Fired Condensing Boilers` == "yes" |
        `Oil Fired Condensing Boilers` == "yes" ~ "Condensing Regular Boiler",
      # Previously had solar PV and thermal for this????
      `Biomass Boiler` == "yes" | 
        `Biomass Boiler Back Boiler` == "yes" |
        `Gas Boiler LPG` == "yes" ~ "Boiler",
      `District Heating` == "yes" ~ "Community",
      TRUE ~ "Unknown"
    ),
    WHS_RENEWABLES = case_when(
      `Air Source Heat Pump` == "yes" ~ "ASHP",
      `Biomass Boiler` == "yes" | 
        `Biomass Boiler Back Boiler` == "yes" ~ "BIO_BOILER",
      `Solar PV` == "yes" ~ "SOLAR_PV",
      `Solar Thermal` == "yes" ~ "SOLAR_THERMAL",
      `Domestic Battery Storage` == "yes" ~ "BATTERY",
      TRUE ~ "Unknown"
    ),
    # Added in floor insulation 02/2025
    WHS_FLOOR_INS = case_when(
      `Floor Insulation` == "yes" ~ "Floor Insulation (Unknown Floor Type)",
      `Robotically Applied Under Floor` == "yes" |
        `Robotically Applied Underfloor Insulation Top Up` == "yes" ~ "Suspended Floor Insulation",
      TRUE ~ "Unknown"
    ),
    WHS_PRE_SAP_SCORE = as.numeric(`Pre Sap Rating (EPCDATA::PreInstallSAP)`),
    WHS_POST_SAP_SCORE = as.numeric(`Post Sap Rating (EPCDATA::PostEPCSAPXML)`),
    WHS_PRE_FUEL_BILL = as.numeric(`Base Cost (EPCDATA::FuelCost)`),
    WHS_POST_FUEL_BILL = as.numeric(`End Cost (EPCDATA::PostEPCTotalFuelBillXML)`),
    WHS_POST_CO2_EMISSIONS = as.numeric(`End CO2 (EPCDATA::PostEPCCO2XML)`)
  ) %>%
  select(UPRN, TIMESTAMP, YEAR, SOURCE,
         WHS_PROP_TYPE, WHS_TENURE, WHS_LOFT_INS, WHS_PROP_AGE, 
         WHS_WALL_INS, WHS_GLAZ_TYPE, WHS_FUEL_TYPE, WHS_FUEL_SYSTEM, 
         WHS_FLOOR_INS, WHS_FLAT_ROOF_INS, WHS_RENEWABLES,
         WHS_PRE_SAP_SCORE, WHS_POST_SAP_SCORE, WHS_PRE_FUEL_BILL, 
         WHS_POST_FUEL_BILL, WHS_POST_CO2_EMISSIONS)

#########################################################################################################################
#2. Join HEEPS ABS & WHS to pre-2015 HEED records
# TODO: Why do we do each table one by one? Cant we do one load and one rbind?

#Read in attribute name lookup
# TODO: Nice to have a dictionary but why do we bring it in?
connect = odbcConnect('EST_HEED_30_Presentation')
attribute_lookup = sqlQuery(connect, 'SELECT DETAIL_LOOKUP_CODE, DETAIL_LOOKUP_NAME, LOOKUP_NAME,
                          LOOKUP_GROUP_NAME FROM dbo.D_MEASURES;')

attribute_lookup$DETAIL_LOOKUP_NAME = as.character(attribute_lookup$DETAIL_LOOKUP_NAME)
attribute_lookup$LOOKUP_NAME = as.character(attribute_lookup$LOOKUP_NAME)
attribute_lookup$LOOKUP_GROUP_NAME = as.character(attribute_lookup$LOOKUP_GROUP_NAME)

###### 1. Property type ######
#There are no property type records from HEEPS ABS or WHS but this part of the script still needs to be run to bring in old HEED records

#Read in old HEED data (pre-2015)
# NOTE: Added the constant SOURCE and TIMESTAMP values in the import rather than additional lines in R
# Also only bring in the columns we need rather than remove them later after bringing them in
# Don't need to SELECT columns that are used for filtering

#! Using CONVERT not STR_TO_DATE to be SSMS compliant
connect = odbcConnect('EST_HEED_30_Presentation')
homes = sqlQuery(connect, "SELECT 
                 ab.UPRN,
                 CONVERT(DATE, '2000/12/31', 102) AS TIMESTAMP,
                 heed.PROPERTY_TYPE_NAME,
                 SOURCE = 'HEED Pre-2015'
                 from
                 (
                 SELECT
                 heed.HOME_ID,
                 heed.PROPERTY_TYPE_CODE,
                 meas.DETAIL_LOOKUP_NAME as PROPERTY_TYPE_NAME,
                 heed.GOV_REG_CODE
                 from
                 EST_HEED_30_Presentation.dbo.F_HOMES heed
                 inner join EST_HEED_30_Presentation.dbo.D_MEASURES meas on
                 heed.PROPERTY_TYPE_CODE = meas.DETAIL_LOOKUP_CODE) heed
                 inner join
                 (SELECT * from EST_RefData_30_Presentation.dbo.R_ADDRBASE_XREF_NSPD201411_AB201501) ab on
                 heed.HOME_ID = ab.HEED_HOME_ID
                 where heed.GOV_REG_CODE in ('S99999999');")

# Prepare HEEPS for rbind
# TODO: Just rename HEEPS and WHS columns to match the eventual format and then the 
# row binds will just grab those columns anyway is we have the HEED DB as the head
heeps_data <- final_heeps %>%
  select(UPRN, TIMESTAMP, PROPERTY_TYPE_NAME = HEEPS_PROP_TYPE, SOURCE)

#Prepare WHS for rbind
whs_data <- whs %>%
  select(UPRN, TIMESTAMP, PROPERTY_TYPE_NAME = WHS_PROP_TYPE, SOURCE)

#Rbind objects
ptype <- homes %>% 
  bind_rows(heeps_data, whs_data) %>%
  filter(PROPERTY_TYPE_NAME != "Unknown") %>%
  arrange(UPRN, desc(TIMESTAMP))

num_duplicates <- sum(duplicated(ptype$UPRN))
cat("Number of duplicates before removal:", num_duplicates, "\n")

ptype <- ptype %>%
  distinct(UPRN, .keep_all = TRUE) %>%
  filter(!is.na(UPRN)) # Last layer of defence against the dreaded NULLs

#Quick check we have no NAs or weird values
table(ptype$PROPERTY_TYPE_NAME, useNA = "ifany")

rm(whs_data, heeps_data, homes)

# TODO: Needs to be CSV for the SSMS upload?
write.csv(ptype, file=paste(local_path,"/HEED_ptype.csv", sep=""), row.names = FALSE, quote = FALSE)

###### 2. Property tenure ######
#Read in old HEED data (pre-2015)
homes = sqlQuery(connect, "SELECT 
                 ab.UPRN,
                 CASE 
                    WHEN YEAR(CONVERT(DATE, heed.DETAIL_DATE)) > 2015 THEN CONVERT(DATE, '2000-01-01', 102)
                    ELSE CONVERT(DATE, heed.DETAIL_DATE, 102)
                 END AS TIMESTAMP,              
                 heed.TENURE_NAME,
                 SOURCE = 'HEED Pre-2015'
                 from
                 (
                 SELECT
                 heed.HOME_ID,
                 heed.TENURE_CODE,
                 meas.DETAIL_LOOKUP_NAME as TENURE_NAME,
                 hhd.DETAIL_DATE,
                 heed.GOV_REG_CODE
                 from
                 EST_HEED_30_Presentation.dbo.F_HOMES heed
                 inner join EST_HEED_30_Presentation.dbo.D_MEASURES meas on
                 heed.TENURE_CODE = meas.DETAIL_LOOKUP_CODE
                 inner join EST_HEED_20_Enterprise.dbo.HEED_HOME_DETAILS hhd on
                 heed.HOME_ID = hhd.HOMES_HOME_ID
                 and heed.TENURE_CODE = hhd.DETS_LKP_DET_LKP_ID) heed
                 inner join
                 (SELECT * from EST_RefData_30_Presentation.dbo.R_ADDRBASE_XREF_NSPD201411_AB201501) ab on
                 heed.HOME_ID = ab.HEED_HOME_ID
                 where heed.GOV_REG_CODE in ('S99999999');")

#Prepare HEEPS for rbind
heeps_data <- final_heeps %>%
  select(UPRN, TIMESTAMP, TENURE_NAME = HEEPS_TENURE, SOURCE)

#Prepare WHS for rbind
whs_data <- whs %>%
  select(UPRN, TIMESTAMP, TENURE_NAME = WHS_TENURE, SOURCE)

#Rbind objects
pten <- homes %>% 
  bind_rows(heeps_data, whs_data) %>%
  filter(!(TENURE_NAME %in% c("Unknown", "Unknown Tenure", "Other") | is.na(TENURE_NAME))) %>%
  arrange(UPRN, desc(TIMESTAMP))

num_duplicates <- sum(duplicated(pten$UPRN))
cat("Number of duplicates before removal:", num_duplicates, "\n")

pten <- pten %>%
  distinct(UPRN, .keep_all = TRUE) %>%
  filter(!is.na(UPRN)) # Last layer of defence against the dreaded NULLs

#Quick check we have no NAs or weird values
table(pten$TENURE_NAME, useNA = "ifany")

table(pten$SOURCE, useNA = "ifany")

rm(whs_data, heeps_data, homes)

# TODO: Needs to be CSV for the SSMS upload?
write.csv(pten, file=paste(local_path,"/HEED_pten.csv", sep=""), row.names = FALSE, quote = FALSE)

##### 3. Property age ##### 
#Read in old HEED data (pre-2015)
homes = sqlQuery(connect, "SELECT
                 ab.UPRN,
                 CASE 
                    WHEN YEAR(CONVERT(DATE, heed.DETAIL_DATE)) > 2015 THEN CONVERT(DATE, '2000-01-01', 102)
                    ELSE CONVERT(DATE, heed.DETAIL_DATE, 102)
                 END AS TIMESTAMP,              
                 heed.PROPERTY_AGE_NAME,
                 SOURCE = 'HEED Pre-2015'
                 from
                 (
                 SELECT
                 heed.HOME_ID,
                 heed.PROPERTY_AGE_CODE,
                 meas.DETAIL_LOOKUP_NAME as PROPERTY_AGE_NAME,
                 hhd.DETAIL_DATE,
                 heed.GOV_REG_CODE
                 from
                 EST_HEED_30_Presentation.dbo.F_HOMES heed
                 inner join EST_HEED_30_Presentation.dbo.D_MEASURES meas on
                 heed.PROPERTY_AGE_CODE = meas.DETAIL_LOOKUP_CODE
                 inner join EST_HEED_20_Enterprise.dbo.HEED_HOME_DETAILS hhd on
                 heed.HOME_ID = hhd.HOMES_HOME_ID
                 and heed.PROPERTY_AGE_CODE = hhd.DETS_LKP_DET_LKP_ID) heed
                 inner join
                 (SELECT * from EST_RefData_30_Presentation.dbo.R_ADDRBASE_XREF_NSPD201411_AB201501) ab on
                 heed.HOME_ID = ab.HEED_HOME_ID
                 where heed.GOV_REG_CODE in ('S99999999');")

#Prepare HEEPS for rbind
heeps_data <- final_heeps %>%
  select(UPRN, TIMESTAMP, PROPERTY_AGE_NAME = HEEPS_PROP_AGE, SOURCE)

#Prepare WHS for rbind
whs_data <- whs %>%
  select(UPRN, TIMESTAMP, PROPERTY_AGE_NAME = WHS_PROP_AGE, SOURCE)

#Rbind objects
page <- homes %>% 
  bind_rows(heeps_data, whs_data) %>%
  filter(!(PROPERTY_AGE_NAME %in% c("Unknown", "Unknown Built Date", "Other") | is.na(PROPERTY_AGE_NAME))) %>%
  arrange(UPRN, desc(TIMESTAMP))

num_duplicates <- sum(duplicated(page$UPRN))
cat("Number of duplicates before removal:", num_duplicates, "\n")

page <- page %>%
  distinct(UPRN, .keep_all = TRUE) %>%
  filter(!is.na(UPRN)) # Last layer of defence against the dreaded NULLs

#Quick check we have no NAs or weird values
table(page$PROPERTY_AGE_NAME, useNA = "ifany")

table(page$SOURCE, useNA = "ifany")

rm(whs_data, heeps_data, homes)

# Save locally for speed and can always upload to somewhere centrally or change this to be a network drive and take a little longer
write.csv(page, file=paste(local_path,"/HEED_page.csv", sep=""), row.names = FALSE, quote = FALSE)

##### 4. Loft insulation #####
#Read in old HEED data (pre-2015)
homes = sqlQuery(connect, "SELECT 
                 ab.UPRN,
                 CASE 
                    WHEN YEAR(CONVERT(DATE, heed.DETAIL_DATE)) > 2015 THEN CONVERT(DATE, '2000-01-01', 102)
                    ELSE CONVERT(DATE, heed.DETAIL_DATE, 102)
                 END AS TIMESTAMP,              
                 heed.LOFT_INSULATION_NAME,
                 SOURCE = 'HEED Pre-2015'
                 from
                 (
                 SELECT
                 heed.HOME_ID,
                 heed.LOFT_INSULATION_CODE,
                 meas.DETAIL_LOOKUP_NAME as LOFT_INSULATION_NAME,
                 hhd.DETAIL_DATE,
                 heed.GOV_REG_CODE
                 from
                 EST_HEED_30_Presentation.dbo.F_HOMES heed
                 inner join EST_HEED_30_Presentation.dbo.D_MEASURES meas on
                 heed.LOFT_INSULATION_CODE = meas.DETAIL_LOOKUP_CODE
                 inner join EST_HEED_20_Enterprise.dbo.HEED_HOME_DETAILS hhd on
                 heed.HOME_ID = hhd.HOMES_HOME_ID
                 and heed.LOFT_INSULATION_CODE = hhd.DETS_LKP_DET_LKP_ID) heed
                 inner join
                 (SELECT * from EST_RefData_30_Presentation.dbo.R_ADDRBASE_XREF_NSPD201411_AB201501) ab on
                 heed.HOME_ID = ab.HEED_HOME_ID
                 where heed.GOV_REG_CODE in ('S99999999');")

#Prepare HEEPS for rbind
heeps_data <- final_heeps %>%
  select(UPRN, TIMESTAMP, LOFT_INSULATION_NAME = HEEPS_LOFT_INS, SOURCE)

#Prepare WHS for rbind
whs_data <- whs %>%
  select(UPRN, TIMESTAMP, LOFT_INSULATION_NAME = WHS_LOFT_INS, SOURCE)

#Rbind objects
lins <- homes %>% 
  bind_rows(heeps_data, whs_data) %>%
  filter(!(LOFT_INSULATION_NAME %in% c("Unknown", "Loft Insulation Unknown", "Other") | is.na(LOFT_INSULATION_NAME))) %>%
  arrange(UPRN, desc(TIMESTAMP))

num_duplicates <- sum(duplicated(lins$UPRN))
cat("Number of duplicates before removal:", num_duplicates, "\n")

lins <- lins %>%
  distinct(UPRN, .keep_all = TRUE) %>%
  filter(!is.na(UPRN)) # Last layer of defence against the dreaded NULLs

#Quick check we have no NAs or weird values
table(lins$LOFT_INSULATION_NAME, useNA = "ifany")

table(lins$SOURCE, useNA = "ifany")

rm(whs_data, heeps_data, homes)

# Save locally for speed and can always upload to somewhere centrally or change this to be a network drive and take a little longer
write.csv(lins, file=paste(local_path,"/HEED_lins.csv", sep=""), row.names = FALSE, quote = FALSE)

##### 5. External wall type #####
#Read in old HEED data (pre-2015)
# TODO: Is there really no timestamp... How is there one for other tables to join to but not these?
homes = sqlQuery(connect, "SELECT
                 ab.UPRN,
                 CONVERT(DATE, '2000/12/31', 102) AS TIMESTAMP,       
                 heed.WALL_CONSTRUCTION_NAME,
                 SOURCE = 'HEED Pre-2015'
                 from
                 (
                 SELECT
                 heed.HOME_ID,
                 heed.EXTERNAL_WALL_TYPE_CODE as WALL_CONSTRUCTION_CODE,
                 meas.DETAIL_LOOKUP_NAME as WALL_CONSTRUCTION_NAME,
                 heed.GOV_REG_CODE
                 from
                 EST_HEED_30_Presentation.dbo.F_HOMES heed
                 inner join EST_HEED_30_Presentation.dbo.D_MEASURES meas on
                 heed.EXTERNAL_WALL_TYPE_CODE = meas.DETAIL_LOOKUP_CODE
                 ) heed
                 inner join
                 (SELECT * from EST_RefData_30_Presentation.dbo.R_ADDRBASE_XREF_NSPD201411_AB201501) ab on
                 heed.HOME_ID = ab.HEED_HOME_ID
                 where heed.GOV_REG_CODE in ('S99999999');")

#Prepare HEEPS for rbind
heeps_data <- final_heeps %>%
  select(UPRN, TIMESTAMP, WALL_CONSTRUCTION_NAME = HEEPS_WALL_INS, SOURCE)

#Prepare WHS for rbind
whs_data <- whs %>%
  select(UPRN, TIMESTAMP, WALL_CONSTRUCTION_NAME = WHS_WALL_INS, SOURCE)

#Rbind objects
wins <- homes %>% 
  bind_rows(heeps_data, whs_data) %>%
  # TODO: no removing unknown wall type? Or are we mostly after the ins status?
  # DG decided to remove Unknown Insulation -- no point having it. Doesnt help.
  filter(!(WALL_CONSTRUCTION_NAME %in% c("Unknown", "Other", "Unknown Insulation") | is.na(WALL_CONSTRUCTION_NAME))) %>%
  arrange(UPRN, desc(TIMESTAMP))

num_duplicates <- sum(duplicated(wins$UPRN))
cat("Number of duplicates before removal:", num_duplicates, "\n")

wins <- wins %>%
  distinct(UPRN, .keep_all = TRUE) %>%
  filter(!is.na(UPRN)) # Last layer of defence against the dreaded NULLs

#Quick check we have no NAs or weird values
table(wins$WALL_CONSTRUCTION_NAME, useNA = "ifany")

table(wins$SOURCE, useNA = "ifany")

rm(whs_data, heeps_data, homes)

# Save locally for speed and can always upload to somewhere centrally or change this to be a network drive and take a little longer
write.csv(wins, file=paste(local_path,"/HEED_wins.csv", sep=""), row.names = FALSE, quote = FALSE)

##### 6. Main heating fuel ##### 
homes = sqlQuery(connect, "SELECT
                 ab.UPRN,
                 CASE 
                    WHEN YEAR(CONVERT(DATE, heed.DETAIL_DATE)) > 2015 THEN CONVERT(DATE, '2000-01-01', 102)
                    ELSE CONVERT(DATE, heed.DETAIL_DATE, 102)
                 END AS TIMESTAMP,            
                 heed.MAIN_HEATING_FUEL_NAME,
                 SOURCE = 'HEED Pre-2015'
                 FROM
                 (
                 SELECT
                 heed.HOME_ID,
                 heed.MAIN_HEATING_FUEL_CODE as MAIN_HEATING_FUEL_CODE,
                 meas.DETAIL_LOOKUP_NAME as MAIN_HEATING_FUEL_NAME,
                 hhd.DETAIL_DATE,
                 heed.GOV_REG_CODE
                 FROM
                 EST_HEED_30_Presentation.dbo.F_HOMES heed
                 inner join EST_HEED_30_Presentation.dbo.D_MEASURES meas on
                 heed.MAIN_HEATING_FUEL_CODE = meas.DETAIL_LOOKUP_CODE
                 inner join EST_HEED_20_Enterprise.dbo.HEED_HOME_DETAILS hhd on
                 heed.HOME_ID = hhd.HOMES_HOME_ID
                 and heed.MAIN_HEATING_FUEL_code = hhd.DETS_LKP_DET_LKP_ID) heed
                 inner join
                 (SELECT * FROM EST_RefData_30_Presentation.dbo.R_ADDRBASE_XREF_NSPD201411_AB201501) ab on
                 heed.HOME_ID = ab.HEED_HOME_ID
                 WHERE heed.GOV_REG_CODE in ('S99999999');")

#Prepare HEEPS for rbind
heeps_data <- final_heeps %>%
  select(UPRN, TIMESTAMP, MAIN_HEATING_FUEL_NAME = HEEPS_FUEL_TYPE, SOURCE)

#Prepare WHS for rbind
whs_data <- whs %>%
  select(UPRN, TIMESTAMP, MAIN_HEATING_FUEL_NAME = WHS_FUEL_TYPE, SOURCE)

#Rbind objects
ftype <- homes %>% 
  bind_rows(heeps_data, whs_data) %>%
  # TODO no removing unknown wall type? Or are we mostly after the ins status?
  filter(!(MAIN_HEATING_FUEL_NAME %in% c("Unknown", "Other") | is.na(MAIN_HEATING_FUEL_NAME))) %>%
  arrange(UPRN, desc(TIMESTAMP))

num_duplicates <- sum(duplicated(ftype$UPRN))
cat("Number of duplicates before removal:", num_duplicates, "\n")

ftype <- ftype %>%
  distinct(UPRN, .keep_all = TRUE) %>%
  filter(!is.na(UPRN)) # Last layer of defence against the dreaded NULLs

#Quick check we have no NAs or weird values
table(ftype$MAIN_HEATING_FUEL_NAME, useNA = "ifany")

table(ftype$SOURCE, useNA = "ifany")

rm(whs_data, heeps_data, homes)

# Save locally for speed and can always upload to somewhere centrally or change this to be a network drive and take a little longer
write.csv(ftype, file=paste(local_path,"/HEED_ftype.csv", sep=""), row.names = FALSE, quote = FALSE)

##### 7. Main heating system #####
homes = sqlQuery(connect, "SELECT
                 ab.UPRN,
                 CASE 
                    WHEN YEAR(CONVERT(DATE, heed.DETAIL_DATE)) > 2015 THEN CONVERT(DATE, '2000-01-01', 102)
                    ELSE CONVERT(DATE, heed.DETAIL_DATE, 102)
                 END AS TIMESTAMP,         
                 heed.MAIN_HEATING_SYSTEM_NAME,
                 SOURCE = 'HEED Pre-2015'
                 from
                 (
                 SELECT
                 heed.HOME_ID,
                 heed.MAIN_HEATING_SYSTEM_CODE as MAIN_HEATING_SYSTEM_CODE,
                 meas.DETAIL_LOOKUP_NAME as MAIN_HEATING_SYSTEM_NAME,
                 hhd.DETAIL_DATE,
                 heed.GOV_REG_CODE
                 from
                 EST_HEED_30_Presentation.dbo.F_HOMES heed
                 inner join EST_HEED_30_Presentation.dbo.D_MEASURES meas on
                 heed.MAIN_HEATING_SYSTEM_CODE = meas.DETAIL_LOOKUP_CODE
                 inner join EST_HEED_20_Enterprise.dbo.HEED_HOME_DETAILS hhd on
                 heed.HOME_ID = hhd.HOMES_HOME_ID
                 and heed.MAIN_HEATING_SYSTEM_CODE = hhd.DETS_LKP_DET_LKP_ID) heed
                 inner join
                 (SELECT * from EST_RefData_30_Presentation.dbo.R_ADDRBASE_XREF_NSPD201411_AB201501) ab on
                 heed.HOME_ID = ab.HEED_HOME_ID
                 where heed.GOV_REG_CODE in ('S99999999');")

#Prepare HEEPS for rbind
heeps_data <- final_heeps %>%
  select(UPRN, TIMESTAMP, MAIN_HEATING_SYSTEM_NAME = HEEPS_FUEL_SYSTEM, SOURCE)

#Prepare WHS for rbind
whs_data <- whs %>%
  select(UPRN, TIMESTAMP, MAIN_HEATING_SYSTEM_NAME = WHS_FUEL_SYSTEM, SOURCE)

#Rbind objects
system <- homes %>% 
  bind_rows(heeps_data, whs_data) %>%
  # TODO no removing unknown wall type? Or are we mostly after the ins status?
  filter(!(MAIN_HEATING_SYSTEM_NAME %in% c("Unknown", "Other") | is.na(MAIN_HEATING_SYSTEM_NAME))) %>%
  arrange(UPRN, desc(TIMESTAMP))

num_duplicates <- sum(duplicated(system$UPRN))
cat("Number of duplicates before removal:", num_duplicates, "\n")

system <- system %>%
  distinct(UPRN, .keep_all = TRUE) %>%
  filter(!is.na(UPRN)) # Last layer of defence against the dreaded NULLs

#Quick check we have no NAs or weird values
table(system$MAIN_HEATING_SYSTEM_NAME, useNA = "ifany")

table(system$SOURCE, useNA = "ifany")

rm(whs_data, heeps_data, homes)

# Save locally for speed and can always upload to somewhere centrally or change this to be a network drive and take a little longer
write.csv(system, file=paste(local_path,"/HEED_system.csv", sep=""), row.names = FALSE, quote = FALSE)

##### 8. Glazing type #####
homes = sqlQuery(connect, "SELECT 
                 ab.UPRN,
                 CASE 
                    WHEN YEAR(CONVERT(DATE, heed.DETAIL_DATE)) > 2015 THEN CONVERT(DATE, '2000-01-01', 102)
                    ELSE CONVERT(DATE, heed.DETAIL_DATE, 102)
                 END AS TIMESTAMP,              
                 heed.GLAZING_TYPE_NAME,
                 SOURCE = 'HEED Pre-2015'
                 from
                   (
                     SELECT
                     heed.HOME_ID,
                     heed.GLAZING_TYPE_CODE,
                     meas.DETAIL_LOOKUP_NAME as GLAZING_TYPE_NAME,
                     hhd.DETAIL_DATE,
                     heed.GOV_REG_CODE
                     from
                     EST_HEED_30_Presentation.dbo.F_HOMES heed
                     inner join EST_HEED_30_Presentation.dbo.D_MEASURES meas on
                     heed.GLAZING_TYPE_CODE = meas.DETAIL_LOOKUP_CODE
                     inner join EST_HEED_20_Enterprise.dbo.HEED_HOME_DETAILS hhd on
                     heed.HOME_ID = hhd.HOMES_HOME_ID
                     and heed.GLAZING_TYPE_CODE = hhd.DETS_LKP_DET_LKP_ID
                    ) heed
                     inner join
                     (SELECT * from EST_RefData_30_Presentation.dbo.R_ADDRBASE_XREF_NSPD201411_AB201501) ab on
                     heed.HOME_ID = ab.HEED_HOME_ID
                 where heed.GOV_REG_CODE in ('S99999999');")
# Close the ODBC connection when you're done
odbcClose(connect)

#Prepare HEEPS for rbind
heeps_data <- final_heeps %>%
  select(UPRN, TIMESTAMP, GLAZING_TYPE_NAME = HEEPS_GLAZ_TYPE, SOURCE)

#Prepare WHS for rbind
whs_data <- whs %>%
  select(UPRN, TIMESTAMP, GLAZING_TYPE_NAME = WHS_GLAZ_TYPE, SOURCE)

#Rbind objects
glaz <- homes %>% 
  bind_rows(heeps_data, whs_data) %>%
  # TODO no removing unknown wall type? Or are we mostly after the ins status?
  filter(!(GLAZING_TYPE_NAME %in% c("Unknown", "Other") | is.na(GLAZING_TYPE_NAME))) %>%
  arrange(UPRN, desc(TIMESTAMP))

num_duplicates <- sum(duplicated(glaz$UPRN))
cat("Number of duplicates before removal:", num_duplicates, "\n")

glaz <- glaz %>%
  distinct(UPRN, .keep_all = TRUE) %>%
  filter(!is.na(UPRN)) # Last layer of defence against the dreaded NULLs

#Quick check we have no NAs or weird values
table(glaz$GLAZING_TYPE_NAME, useNA = "ifany")

table(glaz$SOURCE, useNA = "ifany")

rm(whs_data, heeps_data, homes)

# Save locally for speed and can always upload to somewhere centrally or change this to be a network drive and take a little longer
write.csv(glaz, file=paste(local_path,"/HEED_glaz.csv", sep=""), row.names = FALSE, quote = FALSE)

###### 9. SAP Rating ######
# Only WHS with this data
sap = whs %>%
  select(UPRN, TIMESTAMP, WHS_PRE_SAP_SCORE, WHS_POST_SAP_SCORE, SOURCE) %>%
  # TODO: Definitely dont use the PRE SAP Score???? We know its had work done so it's not that useful????
  mutate(SAP_SCORE = case_when(
    is.na(WHS_POST_SAP_SCORE) ~ WHS_PRE_SAP_SCORE,
    TRUE ~ WHS_POST_SAP_SCORE
  )) %>%
  filter(!is.na(SAP_SCORE)) %>%
  arrange(UPRN, desc(TIMESTAMP))

cat("Number of duplicates before removal:", sum(duplicated(sap$UPRN)), "\n")

sap <- sap %>%
  distinct(UPRN, .keep_all = TRUE) %>%
  filter(!is.na(UPRN)) %>% # Last layer of defence against the dreaded NULLs
  select(!c(WHS_PRE_SAP_SCORE, WHS_POST_SAP_SCORE))

table(sap$SAP_SCORE, useNA = "ifany")
table(sap$SOURCE)

write.csv(sap,file=paste(local_path,"/HEED_sap.csv", sep=""), row.names = FALSE, quote = FALSE)

##### 10. Fuel Bill #####
# Only WHS with this data
bill = whs %>%
  select(UPRN, TIMESTAMP, WHS_PRE_FUEL_BILL, WHS_POST_FUEL_BILL, SOURCE) %>%
  # TODO: Definitely dont use the PRE fuel bill???? We know its had work done so it's not that useful????
  mutate(SAP_FUEL_BILL = case_when(
    is.na(WHS_POST_FUEL_BILL) ~ WHS_PRE_FUEL_BILL,
    TRUE ~ WHS_POST_FUEL_BILL
  )) %>%
  filter(!is.na(SAP_FUEL_BILL)) %>%
  arrange(UPRN, desc(TIMESTAMP))

cat("Number of duplicates before removal:", sum(duplicated(bill$UPRN)), "\n")

bill <- bill %>%
  distinct(UPRN, .keep_all = TRUE) %>%
  filter(!is.na(UPRN)) %>% # Last layer of defence against the dreaded NULLs
  select(!c(WHS_PRE_FUEL_BILL, WHS_POST_FUEL_BILL))

table(bill$SAP_FUEL_BILL, useNA = "ifany")
table(bill$SOURCE)

write.csv(bill, file=paste(local_path,"/HEED_bill.csv", sep=""), row.names = FALSE, quote = FALSE)

##### 11. Renewables #####
#Prepare HEEPS for rbind
heeps_data <- final_heeps %>%
  select(UPRN, TIMESTAMP, RENEWABLES = HEEPS_RENEWABLES, SOURCE)

#Prepare WHS for rbind
whs_data <- whs %>%
  select(UPRN, TIMESTAMP, RENEWABLES = WHS_RENEWABLES, SOURCE)

# TODO: Filter out unknowns but dont only take the most recent UPRN because they may have a 
# heat pump from ABS but PV from WHS?
# TODO: Order the columns better 
ren = bind_rows(heeps_data, whs_data) %>%
  filter(!(RENEWABLES %in% c("Unknown", "Other") | is.na(RENEWABLES))) %>%
  filter(!is.na(UPRN))   # Last layer of defence against the dreaded NULLs

ren_tech <- ren %>%
  # Count occurrences of each tech per UPRN - can be multiple since we're mixing a lot of sources so cap at 1
  # This also de-dupes in a way because each UPRN and measure combo gets collapsed into a count
  count(UPRN, RENEWABLES) %>% 
  # pmin() ensures anything above a 1 is changed to 1 and everything else stays as is
  mutate(n = pmin(n, 1)) %>%
  pivot_wider(id_cols = UPRN, names_from = RENEWABLES, values_from = n, values_fill = list(n = 0)) %>%
  # We just want to flag presence. Anything else will just be noise and lots of 0s 
  mutate(RENEWABLE_COUNT = rowSums(across(-UPRN), na.rm = TRUE)) %>%
  mutate(across(-c(UPRN, RENEWABLE_COUNT), ~ ifelse(. == "1", "Yes", "")))

ren_source <- ren %>%
  group_by(UPRN, RENEWABLES) %>%
  # Grab only the first instance for these groups -- like distinct() but we can adapt and be more explicit than distinct()
  # If we want to label mixed etc
  summarize(SOURCE = first(SOURCE), .groups = "drop") %>%
  pivot_wider(id_cols = UPRN, names_from = RENEWABLES, values_from = SOURCE 
              , values_fill = list(SOURCE = "")
              # If multiple sources for the same thing just take the first source
              # Only needed without distinct()
              # TODO: Could we say mixed or multiple?
              # , values_fn = list(Source = first)
              )

ren_timestamp <- ren %>%
  group_by(UPRN, RENEWABLES) %>%
  # Grab only the first instance for these groups -- like distinct() but we can adapt and be more explicit than distinct()
  # If we want to label mixed etc
  summarize(TIMESTAMP = first(TIMESTAMP), .groups = "drop") %>%
  pivot_wider(id_cols = UPRN, names_from = RENEWABLES, values_from = TIMESTAMP 
              , values_fill = list(TIMESTAMP = NA)
              # If multiple sources for the same thing just take the first source
              # Only needed without distinct()
              # TODO: Could we say mixed or multiple?
              # , values_fn = list(Source = first)
  )

# Merge both datasets to get the presence and sources
ren <- ren_tech %>%
  left_join(ren_source, by = "UPRN", suffix = c("", "_SOURCE")) %>%
  left_join(ren_timestamp, by = "UPRN", suffix = c("", "_TIMESTAMP"))

rm(whs_data, heeps_data, ren_tech, ren_source)

# Save locally for speed and can always upload to somewhere centrally or change this to be a network drive and take a little longer
write.csv(ren, file=paste(local_path, "/HEED_ren.csv", sep=""), row.names = FALSE, quote = FALSE)

##### 12. Floor Insulation #####
#Prepare HEEPS for rbind
heeps_data <- final_heeps %>%
  select(UPRN, TIMESTAMP, FLOOR_INSULATION = HEEPS_FLOOR_INS, SOURCE)

#Prepare WHS for rbind
whs_data <- whs %>%
  select(UPRN, TIMESTAMP, FLOOR_INSULATION = WHS_FLOOR_INS, SOURCE)

# TODO: Filter out unknowns but dont only take the most recent UPRN because they may have a 
# heat pump from ABS but PV from WHS?
fins <- bind_rows(heeps_data, whs_data) %>%
  filter(!(FLOOR_INSULATION %in% c("Unknown", "Other") | is.na(FLOOR_INSULATION))) %>%
  filter(!is.na(UPRN))   # Last layer of defence against the dreaded NULLs

cat("Number of duplicates before removal:", sum(duplicated(fins$UPRN)), "\n")

fins <- fins %>%
  distinct(UPRN, .keep_all = TRUE) %>%
  filter(!is.na(UPRN))

#Quick check we have no NAs or weird values
table(fins$FLOOR_INSULATION, useNA = "ifany")
table(fins$SOURCE, useNA = "ifany")

rm(whs_data, heeps_data)

# Save locally for speed and can always upload to somewhere centrally or change this to be a network drive and take a little longer
write.csv(fins, file=paste(local_path, "/HEED_fins.csv", sep=""), row.names = FALSE, quote = FALSE)

##### 13. Flat roof ins #####
#Prepare HEEPS for rbind
heeps_data <- final_heeps %>%
  select(UPRN, TIMESTAMP, FLAT_ROOF_INS = HEEPS_FLAT_ROOF_INS, SOURCE)

#Prepare WHS for rbind
whs_data <- whs %>%
  select(UPRN, TIMESTAMP, FLAT_ROOF_INS = WHS_FLAT_ROOF_INS, SOURCE)

# TODO: Filter out unknowns but dont only take the most recent UPRN because they may have a 
flat_roof <- bind_rows(heeps_data, whs_data) %>%
  filter(!(FLAT_ROOF_INS %in% c("Unknown", "Other") | is.na(FLAT_ROOF_INS))) %>%
  filter(!is.na(UPRN))   # Last layer of defence against the dreaded NULLs

cat("Number of duplicates before removal:", sum(duplicated(flat_roof$UPRN)), "\n")

flat_roof <- flat_roof %>%
  distinct(UPRN, .keep_all = TRUE) %>%
  filter(!is.na(UPRN))

#Quick check we have no NAs or weird values
table(flat_roof$FLAT_ROOF_INS, useNA = "ifany")
table(flat_roof$SOURCE, useNA = "ifany")

rm(whs_data, heeps_data)

# Save locally for speed and can always upload to somewhere centrally or change this to be a network drive and take a little longer
write.csv(flat_roof, file=paste(local_path, "/HEED_flat_roof.csv", sep=""), row.names = FALSE, quote = FALSE)

##### 14. Room in roof ins #####
#Prepare HEEPS -- only HEEPS has this for now
rir <- final_heeps %>%
  select(UPRN, TIMESTAMP, RIR_INS = HEEPS_RIR_INS, SOURCE) %>%
  filter(!(RIR_INS %in% c("Unknown", "Other") | is.na(RIR_INS))) %>%
  filter(!is.na(UPRN))   # Last layer of defence against the dreaded NULLs

cat("Number of duplicates before removal:", sum(duplicated(rir$UPRN)), "\n")

rir <- rir %>%
  distinct(UPRN, .keep_all = TRUE) %>%
  filter(!is.na(UPRN))

#Quick check we have no NAs or weird values
table(rir$RIR_INS, useNA = "ifany")
table(rir$SOURCE, useNA = "ifany")

# Save locally for speed and can always upload to somewhere centrally or change this to be a network drive and take a little longer
write.csv(rir, file=paste(local_path, "/HEED_rir_ins.csv", sep=""), row.names = FALSE, quote = FALSE)

#########################################
##### UPDATE INDIVIDUAL SQL TABLES ######
#########################################
#Update all the dbo.P_HEED_XXXX tables in EST_HEED_30_Presentation using the above CSV files and the stored procedures.
#Make sure to update the file location in each procedure and run them from EST-DIA-01.
#These tables will only contain data for Scotland where a UPRN existed. The next HEED script in the chain will use these
#tables as a starting point and bring in HEED records for England and Wales as well as records in Scotland and Northern
#Ireland (which do not have a UPRN).

##### MAKE A MASTER EXTRACT FOR AN UPDATE #######
# TODO: Could we just do this with some pivoting?
# Create master lookup table with all Scottish UPRNs in HEED -- this is weird!
s_master = rbind(ptype[c("UPRN")], pten[c("UPRN")], page[c("UPRN")], lins[c("UPRN")], wins[c("UPRN")],
                 ftype[c("UPRN")], system[c("UPRN")], glaz[c("UPRN")], sap[c("UPRN")], bill[c("UPRN")], 
                 ren[c("UPRN")], rir[c("UPRN")], flat_roof[c("UPRN")], fins["UPRN"])

s_master = as.data.frame(s_master[!duplicated(s_master$UPRN),])
names(s_master) = c("UPRN")

df_list <- list(
  s_master,
  ptype,
  pten,
  page,
  lins,
  wins,
  ftype,
  system,
  glaz,
  sap,
  bill,
  ren,
  rir,
  fins,
  flat_roof
)

#Bring in values for each variable
s_master <- reduce(df_list, left_join, by = "UPRN")

s_master[is.na(s_master)] <- ""

s_master <- s_master %>%
  select(
    -matches("(SOURCE|TIMESTAMP)")
  )

#Count number of data records for each UPRN
cols_to_use <- !(names(s_master) %in% c("UPRN", "RENEWABLE_COUNT"))

s_master$DATA_COUNT <- rowSums(s_master[ , cols_to_use] != "")

table(s_master$DATA_COUNT)
summary(s_master$DATA_COUNT)

# Clean up for a final time to ensure cleaner data
# Define column indices that need conversion 
# TODO: This is why a dictionary may be useful since we could also use that to loop through or something above
# Or to use as a dict to get a consistent column name from a lookup rather than risk a typo
# If the dict doesnt have it you get an error, but as is it'll only error late or may just run and run
# TODO: Just all columns with an NA why define only some? The txt file wont care that the UPRN is now a string 
# Because everything in that txt file will just be a string
# cols_to_convert <- 2:12
# Convert these columns to character
# s_master[cols_to_convert] <- lapply(s_master[cols_to_convert], as.character)
# Replace NA values with empty strings
# s_master[cols_to_convert][is.na(s_master[cols_to_convert])] <- ""
s_master[is.na(s_master)] <- ""

dupes <- s_master %>% filter(duplicated(UPRN) | duplicated(UPRN, fromLast = T))

### TODO: Safe Haven extract 
# TODO: Needs to rename all the timestamps because right now you just get ALL of them as like TIMESTAMP.x.x.x.x.x.x
s_master_safe_haven <- reduce(df_list, left_join, by = "UPRN")

if(nrow(dupes) > 0){
  stop("Duplicate values in the Scotland Master table. Check input tables for duplicates.")
}

# TODO: Save as parquet or SQLite even? Its just for uploading so maybe fine as long as it gets uploaded fine. 
write.table(s_master, file=paste(local_path, "/HEED_s_master.txt",sep=""), na = "", row.names = FALSE, quote = FALSE, sep = "\t")
write.table(s_master_safe_haven, file=paste(local_path, "/HEED_s_master_safe_haven.txt",sep=""), na = "", row.names = FALSE, quote = FALSE, sep = "\t")
