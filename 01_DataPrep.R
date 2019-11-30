# ingest data from MS ACCESS databases

# TODO: Merge unit/agency info with resources and phenology tables
# Resources: committed is what's important? Also has date for phenology, tidyr::complete
# Loop through each year
# Check accuracy against summary data (do I already have this extracted?)
# Do different ACCESS databases match in column names/format?



library(tidyverse)
library(RODBC) 


connect_to_access_dbi <- function(db_file_path)  {
  # require(DBI)
  # make sure that the file exists before attempting to connect
  if (!file.exists(db_file_path)) {
    stop("DB file does not exist at ", db_file_path)
  }
  # Assemble connection strings
  dbq_string <- paste0("DBQ=", db_file_path)
  driver_string <- "Driver={Microsoft Access Driver (*.mdb, *.accdb)};"
  db_connect_string <- paste0(driver_string, dbq_string)
  
  # myconn <- dbConnect(odbc::odbc(),
  #                     .connection_string = db_connect_string)
  myconn <- odbcDriverConnect(db_connect_string)
  return(myconn)
}

fs <- list.files("C:/Users/wepprict/Desktop/firedata", full.names = TRUE)

test <- connect_to_access_dbi(fs[5])


tables <- sqlTables(test, tableType = "TABLE")$TABLE_NAME
res1 <- sqlFetch(test, "IMSR_UNITS")
head(res1)
# odbcClose(test)

res2 <- sqlFetch(test, "IMSR_UNITS_FIRE_STATS_YTD")
head(res2)

# Merge agency/unit data
res <- left_join(res2, res1, by = c("UN_UNITID" = "UNITID", "UN_USTATE" = "USTATE"))

# Resources used
res3 <- sqlFetch(test, "IMSR_UNIT_RESOURCES")

# Phenology
res3 <- sqlFetch(test, "IMSR_UNITS_FIRE_STATS")


# Using YTD data, selecting last date of reporting for each unit x state
# This is more accurate (but not perfect), using non-YTD fire stats was way off!
# Compared against summary data in NICC annual reports by states
stsumm <- res %>% 
  # filter(UN_USTATE == "AZ", AG_AID == "BLM") %>% 
  group_by(UN_UNITID, UN_USTATE) %>% 
  arrange(YDATE) %>% 
  slice(n()) %>% 
  ungroup() %>% 
  group_by(UN_USTATE, AG_AID) %>% 
  summarise_at(.vars = vars(HCOUNT:WFURBACRES), .funs = sum, na.rm = TRUE)


