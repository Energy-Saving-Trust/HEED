#### NISEP Data Cleaning ####
library(tidyverse)
library(arrow)
library(openxlsx)
library(readxl)
library(duckdb)

path <- "T:/CRM and Digital/Home Analytics and HEED/Project Development/HEED Database Refresh/"
curr_folder <- "NISEP/"

previous_year = as.character(as.integer(format(Sys.Date(), "%y")) - 1)
current_year = format(Sys.Date(), "%y")

FY_num = as.character(paste0(previous_year, current_year))

####### Load in new data #########
list.files(paste0(path, curr_folder, "Data/"))

## Excel file in the data folder -- we only want a specific tab
nisep_domestic_path <- paste0(path, curr_folder, "Data/Raw/NISEP 24-25 - for Sean Lemon - Raw data for extrapolation.xlsx")

# Get the tabs
wb <- loadWorkbook(nisep_domestic_path)
names(wb)

data <- read_xlsx(nisep_domestic_path, 
                   sheet = "Claims Master - Domestic", 
                   # Skip the added headers above the actual headers
                   skip = 1)

# Remove duplicates that dont have postcodes because they have other types of data we dont report
data <- data %>%
  # "Remove" is a column we add manually if we need to make a change to remove a property/record after investigating
  # Better if the NISEP team do it but hey
  # EXAMPLE: 4 Hillview in the 24-25 dataset has 3 cases of loft insulation. May was the true install date
  # August was an incorrect addition to the dataset (with a cost of ~£1,000)
  # March then had another row with a cost of -£1,000 to "correct" and cancel out the August addition...
  filter(is.na(Remove)) %>%
  group_by(`Duplicate address check`) %>%
  mutate(dupe_check = n()) %>%
  ungroup()

# Get the no postcode data to flag to the NISEP team
no_postcodes <- data %>%
  filter((is.na(Postcode) | Postcode %in% c("", " ")) & 
           dupe_check == 1)

# Remove the blanks
data <- data %>%
  filter(!(is.na(Postcode) | Postcode %in% c("", " ")))

# Clean the supporting non-measures data
table(data$`House type`, useNA = "ifany")

data <- data %>%
  mutate(
    PROPERTY_TYPE = case_when(
      tolower(`House type`) == "bungalow" ~ "Bungalow (Unknown Detachment)",
      tolower(`House type`) %in% c("detached", "detached bungalow",
                                   "detached house") ~ "Detached House / Bungalow",
      grepl("(?i)flat|maisonette|apartment", `House type`) ~ "Flat / Maisonette",
      grepl("(?i)end\\s*-?\\s*(terrace|teracced|detached)", `House type`) ~ "End Terrace House / Bungalow",
      grepl("(?i)mid\\s*-?\\s*terrace|terrace[d]?\\s*house", `House type`) ~ "Mid Terrace House / Bungalow",
      grepl("(?i)sem[i]?\\s*-?\\s*detach", `House type`) ~ "Semi Detached House / Bungalow",
      `House type` %in% c("N/A", "0", "") | is.na(`House type`) ~ "Unknown Property Type",
      TRUE ~ "ERROR"
    )
  )

if(sum(data$PROPERTY_TYPE=="ERROR") > 0){
  stop("Some error strings for Property Type")
}

# Tenure
table(data$Tenure, useNA = "ifany")

data <- data %>%
  mutate(
    TENURE = case_when(
      grepl("(?i)owner", Tenure) ~ "Owner Occupied",
      grepl("(?i)housing assoc|H\\.A\\.|social housing", Tenure) ~ "Rented from Housing Association",
      grepl("(?i)private|landlord", Tenure) ~ "Privately Rented",
      tolower(Tenure) %in% c("n/a", "0", "", "other") | 
        is.na(Tenure) ~ "Unknown Tenure",
      TRUE ~ "ERROR"
    )
  )

if(sum(data$TENURE=="ERROR") > 0){
  stop("Some error strings for Tenure")
}

# Property Age
table(data$`Year built`, useNA = "ifany")

data <- data %>%
  mutate(year_built_num = as.numeric(`Year built`)) %>%
  mutate(
    PROPERTY_AGE = case_when(
      year_built_num < 1600 | 
        tolower(`Year built`) %in% c("after 1995", "01 after 1995",
                                     "", " ", "post-1976", "pre 1976",
                                     "1950/1990", "1980's/1990's", 
                                     "1915/1990", "unknown") | 
        is.na(`Year built`) ~ "Unknown Property Age",
      # Lets assume it cant be a home built before 1600 AD -- even that is pushing it imo
      (year_built_num < 1900 & 
         year_built_num > 1600) |
         tolower(`Year built`) %in% c("pre-1900", "09 before 1900", 
                                      "pre 1900", "1700's", "1800's", "pre-1930's",
                                      "1850's", "before 1900", "pre-1900's",
                                      "1800's/1961 (extension)",
                                      "1800's/1996 (renovated)") ~ "Built Pre-1900",
      (year_built_num >= 1900 & 
         year_built_num <= 1929) |
         tolower(`Year built`) %in% c("08 1900-1929", "1900-1929",
                                      "1900's", "1920's") ~ "Built 1900-1929",
      (year_built_num >= 1930 & 
         year_built_num <= 1949) |
         tolower(`Year built`) %in% c("1930's", "07 1930-1949",
                                      "1930-1949", "1940's") ~ "Built 1930-1949",
      (year_built_num >= 1950 & 
         year_built_num <= 1966) |
         tolower(`Year built`) %in% c("1950's", "1950s", "06 1950-1965",
                                      "1960's", "1950-1965", "1950-60",
                                      "1960s") ~ "Built 1950-1966",
      (year_built_num >= 1967 & 
         year_built_num <= 1975) |
         tolower(`Year built`) %in% c("1966-1976", "1970's",
                                      "1960's", "05 1966-1976",
                                      "1970s") ~ "Built 1967-1975",
      (year_built_num >= 1976 & 
         year_built_num <= 1982) |
         tolower(`Year built`) %in% c("1977-1981", 
                                      "04 1977-1981") ~ "Built 1976-1982",
      (year_built_num >= 1983 & 
         year_built_num <= 1990) |
         tolower(`Year built`) %in% c("1982-1990", "1980s",
                                      "1980's", "03 1982-1990") ~ "Built 1983-1990",
      (year_built_num >= 1991 & 
         year_built_num <= 1995) |
        tolower(`Year built`) %in% c("1990's", "1990s",
                                     "02 1991-1995") ~ "Built 1991-1995",
      (year_built_num >= 1996 & 
         year_built_num <= 2002) ~ "Built 1996-2002",
      (year_built_num >= 2003 & 
         year_built_num <= 2006) |
        tolower(`Year built`) %in% c("2000's") ~ "Built 2003-2006",
      (year_built_num >= 2006) ~ "Built Post-2006",
      TRUE ~ "ERROR"
    )
  )

if(sum(data$PROPERTY_AGE=="ERROR") > 0){
  error <- data %>% filter(PROPERTY_AGE == "ERROR")
  stop("Some error strings for Property Age")
}

# Bedrooms
table(data$`No. bedrooms`, useNA = "ifany")

data <- data %>%
  mutate(BEDROOMS = case_when(
    `No. bedrooms` == "1" ~ "1",
    `No. bedrooms` == "2" ~ "2",
    `No. bedrooms` == "3" ~ "3",
    `No. bedrooms` == "4" ~ "4",
    `No. bedrooms` %in% c("5", "5+", "5=", "4 PLUS", "4+",
                          "6", "7", "8", "9", "10") ~ "5+",
    is.na(`No. bedrooms`) |
      `No. bedrooms` %in% c("0", "N/A", "", " ") ~ "Unknown",
    TRUE ~ "ERROR"
    )
  )

if(sum(data$BEDROOMS=="ERROR") > 0){
  error <- data %>% filter(BEDROOMS == "ERROR")
  stop("Some error strings for Bedrooms")
}

# Wall insulation
table(data$`Wall insulation type`, useNA = "ifany")
table(data$`Wall Insulation Category`)

data <- data %>%
  mutate(
    # TODO: Add solid wall type?
    EXTERNAL_WALL_TYPE = case_when(
      grepl("(?i)cavity", `Wall Insulation Category`) ~ "Cavity Wall Filled",
      TRUE ~ "Unknown Insulation"
      ),
    # TODO: Does it make sense to say CWI is based on the build age? Should it not be install age?
    # Or is it saying CWI on X date
    WALL_INS = case_when(
      PROPERTY_AGE %in% c("Built 1900-1929", "Built 1930-1949",
                          "Built 1950-1966", "Built 1967-1975",
                          "Built Pre-1900") & 
        grepl("(?i)cavity", `Wall Insulation Category`) ~ "Cavity Wall Insulation (Pre-1976)",
      PROPERTY_AGE %in% c("Built 1976-1982", "Built 1983-1990", 
                          "Built 1991-1995", "Built 1996-2002",
                          "Built 2003-2006", "Built Post-2006") & 
        grepl("(?i)cavity", `Wall Insulation Category`) ~ "Cavity Wall Insulation (Post-1976)",
      grepl("(?i)cavity", `Wall Insulation Category`) ~ "Cavity Wall Insulation (Unknown Property Age)",
      grepl("(?i)solid|std|external", `Wall Insulation Category`) ~ "Solid Wall Insulation",
      is.na(`Wall Insulation Category`) |
        `Wall Insulation Category` == "N/A" ~ "Unknown",
      TRUE ~ "ERROR"
      ),
    WALL_INS_FILL = case_when(
      grepl("(?i)full|refill", `Wall insulation type`) ~ "Full",
      grepl("(?i)partial", `Wall insulation type`) ~ "Partial",
      grepl("(?i)solid|external", `Wall insulation type`) ~ "Unknown Fill",
      is.na(`Wall insulation type`) |
        tolower(`Wall insulation type`) %in% c("unknown", "n/a", "", " ") ~ "Unknown Fill",
      TRUE ~ "ERROR",
    )
  )

if(sum(data$EXTERNAL_WALL_TYPE=="ERROR") > 0){
  error <- data %>% filter(EXTERNAL_WALL_TYPE == "ERROR")
  stop("Some error strings for Wall type")
}

if(sum(data$WALL_INS=="ERROR") > 0){
  error <- data %>% filter(WALL_INS == "ERROR")
  stop("Some error strings for Wall insulation")
}

if(sum(data$WALL_INS_FILL=="ERROR") > 0){
  error <- data %>% filter(WALL_INS_FILL == "ERROR")
  stop("Some error strings for Wall insulation fill")
}

#Loft insulation
#Since we're not sure if NI needs to report on these categories or just the 
# HEED categories, we will make to 2 loft insulation variables
table(data$`Predominant LI - depth`, useNA = "ifany")

data <- data %>%
  mutate(
    LOFT_INSULATION_NI = case_when(
      # TODO: Regex / finding the range before "mm"? Lowercase too.
      `Predominant LI - depth` %in% c("Loft insulation 0-300mm", 
                                      "Loft Insulation 0-300mm", 
                                      "Loft Insulation 0 - 300mm", 
                                      "Loft Insulation 000 - 300mm",
                                      "Loft Insulation 0.-300mm") ~ "Loft Insulation 0 - 300mm",
      `Predominant LI - depth` %in% c("Loft Insulation 25 - 300mm",
                                      "Loft Insulation 25-300mm") ~ "Loft Insulation 25 - 300mm",
      `Predominant LI - depth` %in% c("Loft Insulation 50-300mm",
                                      "Loft Insulation 50 - 300mm") ~ "Loft Insulation 50 - 300mm",
      `Predominant LI - depth` %in% c("Loft Insulation 100-300mm",
                                      "Loft Insulation 100 - 300mm",
                                      "Loft Insulation 100 -  300mm",
                                      "loft insulation 100-300 mm") ~ "Loft Insulation 100 - 300mm",
      `Predominant LI - depth` %in% c("Loft Insulation 150-300mm",
                                      "Loft Insulation 150 - 300mm",
                                      "150-300mm") ~ "Loft Insulation 150 - 300mm",
      `Predominant LI - depth` %in% c("Loft Insulation 200- 300mm",
                                      "Loft Insulation 200-300mm",
                                      "Loft Insulation 200 -  300mm",
                                      "Loft Insulation 200mm") ~ "Loft Insulation 200 - 300mm",
      is.na(`Predominant LI - depth`) ~ "Unknown",
      TRUE ~ "ERROR"
    ),
    LOFT_INSULATION = case_when(
      `Predominant LI - depth` %in% c("Loft Insulation 0 - 270mm",
                                      "Loft Insulation 25 - 270mm",
                                      "Loft Insulation 50 - 270mm") ~ "Loft Insulation 0 - 270mm",
      `Predominant LI - depth` == "Loft Insulation 100 - 270mm" ~ "Loft Insulation 100 - 270mm",
      `Predominant LI - depth` %in% c("Loft Insulation 100 - 300mm",
                                      "Loft Insulation 100 - 300 mm",
                                      "Loft Insulation 100 -300mm",
                                      "Loft Insulation 100-300mm",
                                      "Loft Insulation 100-300mm", 
                                      "Loft Insulation 150-300mm", 
                                      "Loft Insulation 200- 300mm",
                                      "Loft Insulation 200-300mm") ~ "Loft Insulation 100-300mm",
      # TODO: Why dont we do much in HEED with these? Why are the bands so big?
      `Predominant LI - depth` %in% c("Loft Insulation 0-300mm",
                                      "Loft Insulation 50-300mm") ~ "Loft Insulation 0 - 300mm",
      is.na(`Predominant LI - depth`) ~ "Unknown",
      TRUE ~ "ERROR"
    )
  )

if(sum(data$LOFT_INSULATION_NI=="ERROR") > 0){
  stop("Some error strings for loft ins NI")
  error <- data %>% filter(LOFT_INSULATION_NI == "ERROR")
}

if(sum(data$LOFT_INSULATION=="ERROR") > 0){
  error <- data %>% filter(LOFT_INSULATION == "ERROR")
  stop("Some error strings for loft ins")
}

#Heating fuel
table(data$`Fuel type before install`)
table(data$`New fuel type installed`)

data <- data %>%
  mutate(
    MAIN_HEATING_FUEL = case_when(
      grepl("(?i)biomass|solid|smokeless|coal", `Fuel type before install`) ~ "Solid Fuel",
      grepl("(?i)Electric|e7|economy 7", `Fuel type before install`) ~ "Electric",
      ## Added LPG
      grepl("(?i)lpg", `Fuel type before install`) ~ "LPG",
      grepl("(?i)gas", `Fuel type before install`) ~ "Gas",
      grepl("(?i)\\boil\\b", `Fuel type before install`) ~ "Oil",
      tolower(`Fuel type before install`) %in% c("no heating", "no heat", 
                                                 "no available heat source",
                                                 "", " ", "N/A") |
        is.na(`Fuel type before install`) ~ "Unknown Heating Type",
      TRUE ~ "ERROR"
    ),
    FS_FUEL_AFTER = case_when(
      grepl("(?i)biomass|solid|smokeless|coal",`New fuel type installed`) ~ "Solid Fuel",
      grepl("(?i)Electric|e7|economy 7",`New fuel type installed`) |
        grepl("(?i)heat pump", `New heating type installed`) ~ "Electric",
      grepl("(?i)lpg",`New fuel type installed`) ~ "LPG",
      grepl("(?i)gas",`New fuel type installed`) ~ "Gas",
      grepl("(?i)\\boil\\b",`New fuel type installed`) ~ "Oil",
      tolower(`New fuel type installed`) %in% c("no heating", "no heat", 
                                                 "no available heat source",
                                                 "", " ", "N/A") |
        is.na(`New fuel type installed`) ~ "Unknown Heating Type",
      TRUE ~ "ERROR"
    )
  )

if(sum(data$MAIN_HEATING_FUEL=="ERROR") > 0){
  error <- data %>% filter(MAIN_HEATING_FUEL == "ERROR")
  stop("Some error strings for old heating fuel")
}

if(sum(data$FS_FUEL_AFTER=="ERROR") > 0){
  error <- data %>% filter(FS_FUEL_AFTER == "ERROR")
  stop("Some error strings for new heating fuel")
  }

# Heating system
table(data$`New heating type installed`)

data <- data %>%
  mutate(
    MAIN_HEATING_SYSTEM = case_when(
      grepl("(?i)combination|combi", `New heating type installed`) &
        grepl("(?i)boiler", `New heating type installed`) ~ "Condensing Combination Boiler",
      grepl("(?i)conventional", `New heating type installed`) &
        grepl("(?i)boiler", `New heating type installed`) ~ "Condensing Regular Boiler",
      ## Added LPG
      grepl("(?i)high heat retention|electric storage", `New heating type installed`) ~ "Electric Storage Heater",
      grepl("(?i)system", `New heating type installed`) ~ "System Boiler",
      grepl("(?i)air source", `New heating type installed`) ~ "Air Source Heat Pump",
      is.na(`New heating type installed`) ~ "Unknown Heating Type",
      TRUE ~ "ERROR"
    ))
    
if(sum(data$MAIN_HEATING_SYSTEM=="ERROR") > 0){
  error <- data %>% filter(MAIN_HEATING_SYSTEM == "ERROR")
  stop("Some error strings for new heating system")
}

# Install year
# TODO: Multiple install dates -- which one should we use?
# Could this not just be the year that this data is from?
# data <- data %>%
#   mutate(YEAR = as.Date(`Date installed...26`))

# Hot water jacket (HWJ), hot water tank, rad panels, 
# LEDs, energy monitor, water widget
data <- data %>%
  mutate(
    HOT_WATER_TANK_INSULATION = case_when(
      `Total cost H.W.T. J (£)` > 0 ~ "Hot Water Tank Insulation",
      TRUE ~ "Unknown Hot Water Tank Insulation"
    ),
    RADIATOR_PANELS = case_when(
      `Total cost reflective radiator panels (£)` > 0 ~ "Radiator Panels",
      TRUE ~ "Unknown Radiator Panels"
    ),
    LIGHTING = case_when(
      `No. bulbs installed` > 0 ~ "LEDs",
      TRUE ~ "Unknown"
    ),
    ENERGY_MONITOR = case_when(
      `Total cost Smart Controls (£)` > 0 ~ "Energy Monitor",
      TRUE ~ "Unknown"
    ),
    WATER_WIDGET = case_when(
      `Total cost water widget (£)` > 0 ~ "Water Widget",
      TRUE ~ "Unknown"
    ),
    FY = paste0("FY", FY_num),
    row_num = row_number()
  )

write_parquet(data, paste0(path, "NISEP/Data/Raw/cleaned_raw_NISEP_FY-", FY_num, ".parquet"))

# Make a single MEASURE column as well
# Clean to remove bad data / unknowns
measure <- data %>%
  pivot_longer(
    cols = c(HOT_WATER_TANK_INSULATION, WATER_WIDGET, ENERGY_MONITOR, LIGHTING,
             RADIATOR_PANELS, MAIN_HEATING_SYSTEM, LOFT_INSULATION_NI, WALL_INS),
    names_to = "COLUMN",
    values_to = "MEASURE",
    values_drop_na = TRUE
  )

# Path to the CSV file on the network drive
onspd_path <- "J:/Central resources/ONS/ONSPD.csv"

# Connect to DuckDB (creates an in-memory database)
con <- dbConnect(duckdb::duckdb(), dbdir = ":memory:")

# Direct SQL query with filtering
ni_df <- dbGetQuery(con, paste0("
  SELECT 
    LOWER(REPLACE(pcds, ' ', '')) AS pcds, 
    lad25cd AS LAUA_CODE,
    CASE
      WHEN lad25cd = 'N09000001' THEN 'Antrim and Newtownabbey'
      WHEN lad25cd = 'N09000002' THEN 'Armagh City, Banbridge and Craigavon'
      WHEN lad25cd = 'N09000003' THEN 'Belfast'
      WHEN lad25cd = 'N09000004' THEN 'Causeway Coast and Glens'
      WHEN lad25cd = 'N09000005' THEN 'Derry City and Strabane'
      WHEN lad25cd = 'N09000006' THEN 'Fermanagh and Omagh'
      WHEN lad25cd = 'N09000007' THEN 'Lisburn and Castlereagh'
      WHEN lad25cd = 'N09000008' THEN 'Mid and East Antrim'
      WHEN lad25cd = 'N09000009' THEN 'Mid Ulster'
      WHEN lad25cd = 'N09000010' THEN 'Newry, Mourne and Down'
      WHEN lad25cd = 'N09000011' THEN 'Ards and North Down'
      ELSE 'Unknown'
    END AS LAUA_NAME,
    pcon24cd AS PCON_CODE
  FROM read_csv_auto('", onspd_path, "')
  -- WHERE ctry25cd = 'N92000002' and lad25cd IS NOT NULL
  WHERE pcds LIKE 'BT%'
"))

## Load in PCON_NAME lookup from ONS
pcon_lookup <- read.csv("J:/Central resources/Lookups/PCON_Lookup.csv")

pcon_lookup <- pcon_lookup %>%
  distinct(PCON24CD, .keep_all = T) %>%
  rename(PCON_NAME = PCON24NM)

# Prep final clean data table
final_data <- data %>%
  mutate(pcds = tolower(gsub(" ", "", Postcode))) %>%
  left_join(ni_df, by = join_by("pcds" == "pcds")) %>%
  left_join(pcon_lookup[,c("PCON24CD", "PCON_NAME")], by = join_by("PCON_CODE" == "PCON24CD")) %>%
  left_join(measure[, c("MEASURE", "row_num")], by = "row_num") %>%
  mutate(
    SUB_BUILDING = NA,
    COUNTY = NA,
    HWJ = NA,
    OTHER_MEASURES = NA
  ) %>%
  mutate(MEASURE_GROUP = case_when(
    grepl("(?i)unknown", MEASURE) ~ "Unknown",
    # grepl("(?i)loft insulation", MEASURE) ~ "loft Insulation",
    grepl("(?i)boiler|retention|storage|heat pump|radiator", MEASURE) ~ "Heating",
    grepl("(?i)insulation", MEASURE) ~ "Insulation",
    grepl("(?i)\\bleds\\b", MEASURE) ~ "Lighting",
    grepl("(?i)water widget", MEASURE) ~ "Appliances",
    grepl("(?i)energy monitor", MEASURE) ~ "Other",
    TRUE ~ "ERROR"
  )) %>% 
  filter(MEASURE_GROUP != "Unknown")

if(sum(final_data$MEASURE_GROUP=="ERROR") > 0){
  error <- final_data %>% filter(MEASURE_GROUP == "ERROR")
  stop("Some error strings for measure group tag")
}

if(sum(final_data$LAUA_NAME=="Unknown") | sum(is.na(final_data$LAUA_NAME)) > 0){
  error <- final_data %>% filter(LAUA_NAME == "ERROR" | is.na(LAUA_NAME))
  stop("Some error strings for LA name")
}

## If NA then take the manual postcode logic below: hopefully this will eventually become less of an issue as
# ONSPD is updated but I suspect there will always be some manual work needed...
# TODO: We could use the postcode coordinates maybe but theyre not in ONSPD because theyre not in Code Point yet
# So cant even do a spatial join right now

# Its not a data import/join issue its a wrong postcode in the raw data issue... Maybe do a check at the very start for problem postcodes?

fy_data <- final_data %>%
  select(
    LAUA_NAME,
    PCON_CODE,
    # SUB_BUILDING,
    # BUILDING_NUM,
    # STREET,
    # LOCALITY,
    # TOWN,
    # COUNTY,
    # POSTCODE = Postcode,
    PROPERTY_TYPE,
    TENURE,
    MEASURE,
    MEASURE_GROUP,
    NISEP_SCHEME = `Scheme Name`
    # We dont use these for Lynsey's report...??
    # OTHER_MEASURES,
    # EXTERNAL_WALL_TYPE,
    # WALL_INS_FILL,
    # LOFT_INSULATION_NI,
    # MAIN_HEATING_FUEL,
    # # Why do we have both?
    # FS_FUEL_BEFORE = MAIN_HEATING_FUEL,
    # FS_FUEL_AFTER,
    # MAIN_HEATING_SYSTEM,
    # HWJ,
    # HOT_WATER_TANK_INSULATION,
    # WATER_WIDGET,
    # LIGHTING,
    # RADIATOR_PANELS,
    # ENERGY_MONITOR,
    # # FY,
    # WALL_INS
  ) 

# Write out this year's data
# TODO: Where does this go? 
write.csv(fy_data, file=paste(path,"NISEP/Outputs/ni_fy-", FY_num, ".csv",sep=""), row.names=F)

### Clean other cols and bring into master for storage and DfE
# Bring in the master file and append to the base before prepping for DfE and to save as a master file
# TODO: Figure out which should be the master file...
#Prepare extract for DfE
fy_dfe <- final_data %>%
  select(
    SUB_BUILDING,
    BUILDING_NUM = `House number`,
    STREET = `Address 1`,
    LOCALITY = `Address 2`,
    TOWN = `Address 3`,
    COUNTY,
    POSTCODE = Postcode,
    PCON_CODE,
    PCON_NAME,
    LAUA_CODE,
    LAUA_NAME,
    PROPERTY_TYPE,
    TENURE,
    PROPERTY_AGE,
    MEASURE,
    MEASURE_GROUP,
    FY,
    NISEP_SCHEME = `Scheme Name`
  )

#Remove NAs
fy_dfe[is.na(fy_dfe)]=""
fy_dfe$SUB_BUILDING[grep("N/A", fy_dfe$SUB_BUILDING,value=F)]=""
fy_dfe$TOWN[grep("N/A", fy_dfe$TOWN,value=F)]=""

# Bring in previous extract to append to the bottom
ni_dfe <- read_parquet(paste0(path, "NISEP/Data/dfe_full_heed_data.parquet"))

ni_dfe <- ni_dfe %>% 
  rename(
    LAUA_CODE_OLD = LAUA_CODE,
    LAUA_NAME_OLD = LAUA_NAME,
    PCON_CODE_OLD = PCON_CODE,
    PCON_NAME_OLD = PCON_NAME
  ) %>%
  mutate(pcds = tolower(gsub(" ", "", POSTCODE))) %>%
  left_join(ni_df, by = join_by("pcds" == "pcds")) %>%
  left_join(pcon_lookup[,c("PCON24CD", "PCON_NAME")], by = join_by("PCON_CODE" == "PCON24CD")) %>%
  mutate(
    LAUA_CODE = coalesce(LAUA_CODE, LAUA_CODE_OLD),
    LAUA_NAME = coalesce(LAUA_NAME, LAUA_NAME_OLD),
    PCON_CODE = coalesce(PCON_CODE, PCON_CODE_OLD),
    PCON_NAME = coalesce(PCON_NAME, PCON_NAME_OLD)
  ) %>%
  select(
    SUB_BUILDING,
    BUILDING_NUM,
    STREET,
    LOCALITY,
    TOWN,
    COUNTY,
    POSTCODE,
    PCON_CODE,
    PCON_NAME,
    LAUA_CODE,
    LAUA_NAME,
    PROPERTY_TYPE,
    TENURE,
    PROPERTY_AGE,
    MEASURE,
    MEASURE_GROUP,
    FY,
    NISEP_SCHEME
  ) %>%
  bind_rows(fy_dfe) %>%
  mutate(add = paste0(SUB_BUILDING,
                      BUILDING_NUM,
                      STREET,
                      LOCALITY,
                      TOWN,
                      COUNTY,
                      POSTCODE)) %>%
  mutate(across(
    c(BUILDING_NUM, STREET, LOCALITY, TOWN, COUNTY),
    ~ str_squish(str_to_title(gsub("[,\\s]*$", "", as.character(.x), perl = T)))
  )) %>%
  # mutate(STREET = str_squish(str_to_title(gsub("[,\\s]*$", "", STREET)))) %>%
  arrange(FY, POSTCODE, add) %>%
  select(-add)

# Save these files
write.csv(ni_dfe, file=paste0(path,"NISEP/Reports/DfE HEED Extract - FY2015-", current_year, ".csv"), row.names = F, fileEncoding = "windows-1252")

# Move the previous parquet to Archive with a timestamp
file.rename(paste0(path, "NISEP/Data/dfe_full_heed_data.parquet"), 
            paste0(path, "NISEP/Data/ARCHIVE/dfe_full_heed_data_", Sys.Date(), ".parquet"))
write_parquet(ni_dfe, sink = paste0(path, "NISEP/Data/dfe_full_heed_data.parquet"))

## Historic object -- rather than copy pasting...
ni_dfe_master_hist <- read_parquet(paste0(path, "NISEP/Data/ni_dfe_full_historic.parquet"))
  
fy_dfe_hist <- fy_dfe %>%
  select(
    `Local Authority` = LAUA_NAME,
    `Parliamentary Constituency Code` = PCON_CODE,
    `Property Type` = PROPERTY_TYPE,
    `Property Tenure` = TENURE,
    Measure = MEASURE,
    `Measure Group` = MEASURE_GROUP
  )

ni_dfe_master_hist <- ni_dfe_master_hist %>%
  bind_rows(fy_dfe_hist)

# Save these files
write.csv(ni_dfe_master_hist, file=paste0(path, "NISEP/Reports/DfE HEED Extract - Historic Installations.csv"), row.names = F, fileEncoding = "windows-1252")

# Move the previous parquet to Archive with a timestamp
file.rename(paste0(path, "NISEP/Data/ni_dfe_full_historic.parquet"), 
            paste0(path, "NISEP/Data/ARCHIVE/ni_dfe_full_historic_", Sys.Date(), ".parquet"))
write_parquet(ni_dfe_master_hist, sink = paste0(path, "NISEP/Data/ni_dfe_full_historic.parquet"))
  