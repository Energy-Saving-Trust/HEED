#### NISEP Data Cleaning ####
library(tidyverse)
library(arrow)
library(openxlsx)
library(readxl)

path <- "T:/CRM and Digital/Home Analytics and HEED/Project Development/HEED Database Refresh/"
curr_folder <- "NISEP/"

previous_year = as.character(as.integer(format(Sys.Date(), "%y")) - 1)
current_year = format(Sys.Date(), "%y")

####### Load in new data #########
list.files(paste0(path, curr_folder, "Data/"))

## Excel file in the data folder -- we only want a specific tab
nisep_domestic_path <- paste0(path, curr_folder, "Data/NISEP 24-25 - for Sean Lemon - Raw data for extrapolation.xlsx")

# Get the tabs
wb <- loadWorkbook(nisep_domestic_path)
names(wb)

data <- read_xlsx(nisep_domestic_path, 
                   sheet = "Claims Master - Domestic", 
                   # Skip the added headers above the actual headers
                   skip = 1)

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
  warning("Some error strings for Property Type")
}

# Tenure
table(data$Tenure, useNA = "ifany")

data <- data %>%
  mutate(
    TENURE = case_when(
      grepl("(?i)owner", Tenure) ~ "Bungalow (Unknown Detachment)",
      grepl("(?i)housing assoc|H\\.A\\.|social housing", Tenure) ~ "Rented from Housing Association",
      grepl("(?i)private|landlord", Tenure) ~ "Privately Rented",
      tolower(Tenure) %in% c("n/a", "0", "", "other") | 
        is.na(Tenure) ~ "Unknown Tenure",
      TRUE ~ "ERROR"
    )
  )

if(sum(data$TENURE=="ERROR") > 0){
  warning("Some error strings for Tenure")
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
  warning("Some error strings for Property Age")
  error <- data %>% filter(PROPERTY_AGE == "ERROR")
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
  warning("Some error strings for Bedrooms")
  error <- data %>% filter(BEDROOMS == "ERROR")
}

# Wall insulation
table(data$`Wall insulation type`, useNA = "ifany")

data <- data %>%
  mutate(
    # TODO: Does it make sense to say CWI is based on the build age? Should it not be install age?
    # Or is it saying CWI on X date
    EXTERNAL_WALL_TYPE = case_when(
      `Wall insulation type` == "1" ~ "1",
      `Wall insulation type` == "2" ~ "2",
      `Wall insulation type` == "3" ~ "3",
      `Wall insulation type` %in% c("5", "5+", "5=", "4 PLUS", "4+",
                            "6", "7", "8", "9", "10") ~ "5+",
      is.na(`Wall insulation type`) |
        `Wall insulation type` %in% c("0", "N/A", "", " ") ~ "Unknown",
      TRUE ~ "ERROR"
      ),
    WALL_INS = case_when(
      grepl("(?i)full|refill|std|external", `Wall insulation type`) ~ "Full",
      grepl("(?i)partial", `Wall insulation type`) ~ "Partial",
      tolower(`Wall insulation type`) %in% c("unknown", "n/a", "", " ") ~ "",
      TRUE ~ "ERROR",
    )
  )

if(sum(data$EXTERNAL_WALL_TYPE=="ERROR") > 0){
  warning("Some error strings for Wall type")
  error <- data %>% filter(EXTERNAL_WALL_TYPE == "ERROR")
}

if(sum(data$WALL_INS=="ERROR") > 0){
  warning("Some error strings for Wall insulation")
  error <- data %>% filter(WALL_INS == "ERROR")
}

#Loft insulation
table(data$`Predominant LI - depth`, useNA = "ifany")
m1 <- data %>%
  filter(!is.na(`Predominant LI - depth`) &
           !`Predominant LI - depth` %in% c(0, "0", "N/A")) %>%
  mutate(Measure = case_when(
    # TODO: Regex / finding the range before "mm"? Lowercase too.
    `Predominant LI - depth` %in% c("Loft insulation 0-300mm", 
                                    "Loft Insulation 0-300mm", 
                                    "Loft Insulation 0 - 300mm", 
                                    "Loft Insulation 0.-300mm") ~ "Loft Insulation 0 - 300mm",
    `Predominant LI - depth` %in% c("Loft Insulation 50-300mm") ~ "Loft Insulation 50 - 300mm",
    `Predominant LI - depth` %in% c("Loft Insulation 100-300mm",
                                    "Loft Insulation 100 - 300mm") ~ "Loft Insulation 100 - 300mm",
    `Predominant LI - depth` %in% c("Loft Insulation 150-300mm",
                                    "Loft Insulation 150 - 300mm") ~ "Loft Insulation 150 - 300mm",
    `Predominant LI - depth` %in% c("Loft Insulation 200- 300mm",
                                    "Loft Insulation 200-300mm",
                                    "Loft Insulation 200 -  300mm",
                                    "Loft Insulation 200mm") ~ "Loft Insulation 200 - 300mm",
    TRUE ~ "ERROR"
  ))
  
if(sum(m1$Measure=="ERROR") > 0){
  warning("Some error strings for M1")
}

#Wall insulation
table(data$`Wall Insulation Category`)

m2 <- data %>%
  filter(!is.na(`Wall Insulation Category`) &
           !`Wall Insulation Category` %in% c(0, "0", "N/A")) %>%
  mutate(Measure = case_when(
    `Wall Insulation Category` == "Cavity Wall Insulation" &
      `Year built` < 1976 ~ "Cavity Wall Insulation (Pre-1976)",
    `Wall Insulation Category` == "Cavity Wall Insulation" &
      `Year built` >= 1976 ~ "Cavity Wall Insulation (Post-1976)",
    `Wall Insulation Category` == "Solid Wall Insulation" ~ "Solid Wall Insulation",
    TRUE ~ "ERROR"
  ))

if(sum(m2$Measure=="ERROR") > 0){
  warning("Some error strings for M2")
}

#Heating system
table(data$`New heating type installed`)

m3 <- data %>%
  filter(!is.na(`New heating type installed`) &
           !`New heating type installed` %in% c(0, "0", "N/A")) %>%
  mutate(Measure = case_when(
    grepl("(?i)combination boiler", `New heating type installed`) ~ "Condensing Combination Boiler",
    grepl("(?i)conventional", `New heating type installed`) ~ "Condensing Regular Boiler",
    `New heating type installed` == "Air Source Heat Pump" ~ "Air Source Heat Pump",
    `New heating type installed` == "High Heat Retention Storage Heaters" ~ "High Heat Retention Storage Heaters",
    TRUE ~ "ERROR"
  ))

if(sum(m3$Measure=="ERROR") > 0){
  warning("Some error strings for M3")
}

#Hot water jacket (HWJ)
table(data$`Total cost H.W.T. J (£)`)
        
m4 <- data %>%
  filter(!is.na(`Total cost H.W.T. J (£)`) &
           !`Total cost H.W.T. J (£)` %in% c(0, "0", "N/A")) %>%
  mutate(Measure = "Hot Water Tank Insulation")

#Water widget
table(data$`Total cost water widget (£)`)

m5 <- data %>%
  filter(!is.na(`Total cost water widget (£)`) &
           !`Total cost water widget (£)` %in% c(0, "0", "N/A")) %>%
  mutate(Measure = "Water Widget")

#LEDs
table(data$`No. bulbs installed`)

m6 <- data %>%
  filter(!is.na(`No. bulbs installed`) &
           !`No. bulbs installed` %in% c(0, "0", "N/A")) %>%
  mutate(Measure = "LEDs")

#Heating controls (energy monitoring)
table(data$`Total cost Smart Controls (£)`)

m7 <- data %>%
  filter(!is.na(`Total cost Smart Controls (£)`) &
           !`Total cost Smart Controls (£)` %in% c(0, "0", "N/A")) %>%
  mutate(Measure = "Energy Monitor")

#Rad panels
table(data$`Total cost reflective radiator panels (£)`)

m8 <- data %>%
  filter(!is.na(`Total cost reflective radiator panels (£)`) &
           !`Total cost reflective radiator panels (£)` %in% c(0, "0", "N/A")) %>%
  mutate(Measure = "Radiator Panels")

#Bind together
final = rbind(m1,m2,m3,m4,m5,m6,m7,m8)

length(final$`Scheme Name`)
#6699
#7,965

table(final$Measure)
# Air Source Heat Pump  Cavity Wall Insulation (Post-1976)   Cavity Wall Insulation (Pre-1976) 
# 9                                1173                                 752 
# Condensing Combination Boiler           Condensing Regular Boiler                      Energy Monitor 
# 1061                                 190                                 405 
# High Heat Retention Storage Heaters           Hot Water Tank Insulation                                LEDs 
# 54                                 138                                1602 
# Loft Insulation 0 - 300mm         Loft Insulation 100 - 300mm         Loft Insulation 150 - 300mm 
# 483                                 964                                  78 
# Loft Insulation 200 - 300mm          Loft Insulation 50 - 300mm                     Radiator Panels 
# 51                                 171                                 294 
# Solid Wall Insulation                        Water Widget 
# 8                                 532

final <- final %>%
  mutate(
    SUB_BUILDING = NA,
    County = NA,
    HWJ.Provided. = NA,
    Radiator.Panels. = NA,
    Any.Other.Measures. = NA,
    FY = paste0("FY", previous_year, current_year) 
  ) %>%
  select(
    SUB_BUILDING,
    BUILDING_NUM = `House number`,
    STREET = `Address 1`,
    LOCALITY = `Address 2`,
    TOWN = `Address 3`,
    COUNTY,
    POSTCODE = Postcode,
    PROPERTY_TYPE = `House type`,
    BEDROOMS = `No. bedrooms`,
    TENURE,
    MAIN_HEATING_FUEL = `Fuel type before install`,
    PROPERTY_AGE = `Year built`,
    MEASURE,
    YEAR = `Date installed`,
    NISEP_SCHEME = `Scheme Name`,
    EXTERNAL_WALL_TYPE = `Wall insulation type`,
    FS_FUEL_BEFORE = `Fuel type before install`,
    FS_FUEL_AFTER = `New fuel type installed`,
    HWJ,
    RADIATOR_PANELS,
    OTHER_MEASURES,
    FY
  )

#Save object for import into processing script
saveRDS(final, file=paste0(path,"/Rdata/ni_cleansed_", current_year, ".RDS"))

### Clean other cols and bring into master


# Path to the CSV file on the network drive
onspd_path <- "J:/Central resources/ONS/ONSPD.csv"

# Connect to DuckDB (creates an in-memory database)
con <- dbConnect(duckdb::duckdb(), dbdir = ":memory:")

# Direct SQL query with filtering
ni_df <- dbGetQuery(con, paste0("
  SELECT 
    pcds, 
    lad25cd,
    pcon24cd
  FROM read_csv_auto('", onspd_path, "')
  WHERE ctry25cd = 'N92000002' and lad25cd IS NOT NULL
"))

