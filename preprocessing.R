library(ipumsr)
library(tidyverse)

# Use for styling file based on tidyverse style guide
# Keeps the file manageable and readable
library(styler)
styler:::style_active_file()

# Read IPUMS data files
ddi <- read_ipums_ddi("data/usa_00161.xml")
raw <- read_ipums_micro(ddi)

# Show variable descriptions and levels in an interactive
# document
ipums_view(ddi)

# Create the processed data file including only the final
# output rows
processed <- raw %>%
  transmute(
    SEX = SEX,
    AGE = AGE,
    age_group = case_when(
      AGE <= 17 ~ 1,
      AGE <= 64 ~ 2,
      TRUE ~ 3
    ),
    married = ifelse(
      AGE >= 15,
      case_when(
        MARST == 1 |
          MARST == 2 ~ 2,
        MARST == 6 ~ 1,
        TRUE ~ 3
      ),
      NA
    ),
    househould_size = ifelse(PERNUM == 1 |
      GQ %in% c(3, 4),
    NUMPREC,
    NA
    ),
    race_recoded = case_when(
      HISPAN != 0 ~ 1,
      RACE == 1 ~ 2,
      RACE == 2 ~ 4,
      RACE == 3 ~ 5,
      RACE == 4 | RACE == 5 | RACE == 6 ~ 3,
      RACE == 8 | RACE == 9 ~ 7,
      RACE == 7 ~ 6,
      TRUE ~ 6
    ),
    # TODO: Processing for Latino race once
    #       methodology is finalized
    latino_race = NA,
    latino_origin = NA
  )


# Take a look at the processed data summary to ensure
# computed variables look reasonable
processed %>% summary()
processed %>% str()
