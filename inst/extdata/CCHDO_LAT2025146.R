# CCHDO Template
library(tidyverse)
# read in OCADS file

OCADS_fn <- 'C:/users/ogradye/Documents/local_submissions/data/2025/LAT2025146/LAT2025146_data.csv'
OCADS <- read_csv(OCADS_fn)

# rename BTL_LAT and BTL_LON to latitude and longitude
OCADS <- OCADS %>%
  rename(LATITUDE = BTL_LAT, LONGITUDE = BTL_LON)

# UPDATE DAT FORMAT FROM YYY-MM-DD TO YYYYMMDD
OCADS$DATE <- gsub(as.character(OCADS$DATE), pattern = '-', replacement = '')


# add depth unit
OCADS$DEPTH[1] <- 'METERS'

# remove name column
OCADS <- OCADS %>%
  select(-NAME)

# rename NH3 to NH4
OCADS <- OCADS %>%
  rename(NH4 = NH3)
OCADS <- OCADS %>%
  rename(NH4_FLAG_W = NH3_FLAG_W)

# strip special characters out of station names
OCADS$STNNBR <- gsub('[^[:alnum:]]', '', OCADS$STNNBR)


write_csv(OCADS,
          'C:/users/ogradye/Documents/local_submissions/data/2025/LAT2025146/CCHDO/LAT2025146_data.csv',
           quote = 'none')
