##########################
#                        #
#     HEED EXTRACTOR     #
#                        #
##########################
## Energy Saving Trust
## VCP / NMc
# Extract HEED data into a format for the National Safe Haven
###### Set up environment ######
#load in libraries
library('RODBC')
library(dplyr)
library(readr)
library(arrow)

#set options so that R does not report using scientific notation
options(scipen=999)

#set paths
main_path = "X:/P293 - Data Analysis and Reporting/LOCAL AUTHORITY SUPPORT/Data Requests/5. Scottish Government/HEED_National-Safe-Haven_extracts/"
# write.csv is super slow across network.  Write locally, then manually copy file to network drive.
output_path = "C:/Users/neil.mcCallum/Documents/r_output/"

###### Load, and wrangle the data ######
#load in master table
#SQL_db <- odbcConnect('EST_HEED_30_Presentation')
#master <- sqlQuery(SQL_db, 'SELECT * FROM dbo.P_HEED_SCOTLAND_MASTER;')

conn_str <- "Driver={ODBC Driver 17 for SQL Server};
             Server=produksvmdia01;
             Database=EST_HEED_30_Presentation;
             Trusted_Connection=Yes;"
SQL_db <- odbcDriverConnect(conn_str)
#test the connection
#sqlQuery(SQL_db, "SELECT 1 AS test;")

master <- sqlQuery(SQL_db, 'SELECT * FROM dbo.P_HEED_SCOTLAND_MASTER;')

#load in source & timestamp tables
SQL_tab <- read.csv(paste0(main_path, 'SQL_tables.csv'))
heed <- master
for(i in 1:nrow(SQL_tab)) {
  
  column_data <- sqlQuery(SQL_db,
                          paste0('SELECT * FROM dbo.P_HEED_', SQL_tab$SQL_TABLES[i], ';'))
  
  if (SQL_tab$SQL_TABLES[i] == "RENEWABLES") {
    
    # Correct select() syntax
    column_data <- column_data %>% 
      select(UPRN, 
             BATTERY_TIMESTAMP , BATTERY_SOURCE, 
             SOLAR_PV_TIMESTAMP, SOLAR_PV_SOURCE,
             SOLAR_THERMAL_TIMESTAMP, SOLAR_THERMAL_SOURCE,
             BIO_BOILER_TIMESTAMP, BIO_BOILER_SOURCE,
             ASHP_TIMESTAMP, ASHP_SOURCE)
    
    #Rename columns to standardise
    names(column_data)[names(column_data) == "BATTERY_TIMESTAMP"]       <- "TIMESTAMP_BATTERY"
    names(column_data)[names(column_data) == "BATTERY_SOURCE"]          <- "SOURCE_BATTERY"
    names(column_data)[names(column_data) == "SOLAR_PV_TIMESTAMP"]      <- "TIMESTAMP_SOLAR_PV"
    names(column_data)[names(column_data) == "SOLAR_PV_SOURCE"]         <- "SOURCE_SOLAR_PV"
    names(column_data)[names(column_data) == "SOLAR_THERMAL_TIMESTAMP"] <- "TIMESTAMP_SOLAR_THERMAL"
    names(column_data)[names(column_data) == "SOLAR_THERMAL_SOURCE"]    <- "SOURCE_SOLAR_THERMAL"
    names(column_data)[names(column_data) == "ASHP_TIMESTAMP"]          <- "TIMESTAMP_ASHP"
    names(column_data)[names(column_data) == "ASHP_SOURCE"]             <- "SOURCE_ASHP"
    names(column_data)[names(column_data) == "BIO_BOILER_TIMESTAMP"]    <- "TIMESTAMP_BIO_BOILER"
    names(column_data)[names(column_data) == "BIO_BOILER_SOURCE"]       <- "SOURCE_BIO_BOILER"

    # Join renewables columns
    heed <- left_join(heed, column_data, by = "UPRN")
    
  } else {
    
    # non-renewables
    column_data <- column_data %>% 
      select(UPRN, TIMESTAMP, SOURCE)
    
    names(column_data)[2] <- paste0("TIMESTAMP_", SQL_tab$SQL_TABLES[i])
    names(column_data)[3] <- paste0("SOURCE_", SQL_tab$SQL_TABLES[i])
    
    heed <- left_join(heed, column_data, by = "UPRN")
  }
  
}
odbcClose(SQL_db)

heed_complete <- heed %>% 
  select(UPRN, PROPERTY_TYPE_NAME, TIMESTAMP_PROPERTY_TYPE, 
        SOURCE_PROPERTY_TYPE, TENURE_NAME, TIMESTAMP_PROPERTY_TENURE, 
        SOURCE_PROPERTY_TENURE, PROPERTY_AGE_NAME, TIMESTAMP_PROPERTY_AGE, 
        SOURCE_PROPERTY_AGE, LOFT_INSULATION_NAME, TIMESTAMP_LOFT_INSULATION, 
        SOURCE_LOFT_INSULATION, WALL_CONSTRUCTION_NAME, TIMESTAMP_WALL_CONSTRUCTION, 
        SOURCE_WALL_CONSTRUCTION, MAIN_HEATING_FUEL_NAME, TIMESTAMP_FUEL_TYPE, 
        SOURCE_FUEL_TYPE, MAIN_HEATING_SYSTEM_NAME, TIMESTAMP_HEATING_SYSTEM, 
        SOURCE_HEATING_SYSTEM, GLAZING_TYPE_NAME, TIMESTAMP_GLAZ_TYPE, 
        SOURCE_GLAZ_TYPE, SAP_SCORE, TIMESTAMP_SAP_SCORE, SOURCE_SAP_SCORE, 
        SAP_FUEL_BILL, TIMESTAMP_SAP_FUEL_BILL, SOURCE_SAP_FUEL_BILL,
        BATTERY, TIMESTAMP_BATTERY, SOURCE_BATTERY, 
        SOLAR_PV, TIMESTAMP_SOLAR_PV, SOURCE_SOLAR_PV,
        SOLAR_THERMAL, TIMESTAMP_SOLAR_THERMAL, SOURCE_SOLAR_THERMAL, 
        ASHP, TIMESTAMP_ASHP, SOURCE_ASHP, 
        BIO_BOILER, TIMESTAMP_BIO_BOILER, SOURCE_BIO_BOILER, 
        DATA_COUNT)

# Load in Home Analytics
if (!exists("db_final_form", inherits = FALSE)) {
  db_final_form <- read_parquet(parquet_file)
}
# find UPRNs in HEED but not in Home Analytics
missing_uprn <- heed_complete %>%
  anti_join(db_final_form, by = "UPRN")

count(missing_uprn)   #8966
count(heed_complete)  #1163650

# Remove rows from HEED output that are not in Home Analytics.
heed_complete <- heed_complete %>%
  filter(!(UPRN %in% missing_uprn$UPRN))

count(heed_complete)  #1154684

# find any duplicate uprns
duplicates <- heed_complete %>%
  group_by(UPRN) %>%
  filter(n() > 1) %>%
  ungroup()

count(duplicates) #4304

# remove duplicates. keep first occurance of UPRN.
heed_complete <- heed_complete %>%
  distinct(UPRN, .keep_all = TRUE)

count(heed_complete) #1152448

###### Save ######
write.csv(heed_complete, paste0(output_path, "HEED_Scotland_", Sys.Date(),".csv"), row.names = F)