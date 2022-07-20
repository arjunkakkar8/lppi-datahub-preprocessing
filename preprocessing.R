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
    # JZ: Can you double check this condition below right? I'm not sure it looks right.
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
    latino_origin = NA,
    # Jie's code
    eduatt = ifelse(
      AGE >= 25,
      case_when(
        EDUCD > 1 & EDUCD <= 61 ~ 1,
        EDUCD == 63 | EDUCD == 64 ~ 2,
        EDUCD >= 65 & EDUCD <= 81 ~ 3,
        EDUCD == 101 ~ 4,
        EDUCD > 114 ~ 5
      ),
      NA
    ),
    lep = ifelse(
      AGE >= 5,
      case_when(
        SPEAKENG == 3 ~ 1,
        SPEAKENG == 4 ~ 2,
        SPEAKENG %in% c(1,5,6) ~ 3
      ),
      NA
    ),
    spanish = ifelse(
      AGE >= 5,
      case_when(
        LANGUAGE == 12 ~ 1,
        TRUE ~ 0
      ),
      NA
    ),
    spanish_lep = ifelse(
      AGE >= 5 &
        LANGUAGE == 12,
      case_when(
        SPEAKENG == 4 ~ 1,
        SPEAKENG %in% c(1,5,6) ~ 2
      ),
      NA
    ),
    otherlang = ifelse(
      AGE >= 5,
      case_when(
        LANGUAGE != 12 & LANGUAGE != 1 ~ 1,
        TRUE ~ 0
      ),
      NA
    ),
    otherlang_lep = ifelse(
      AGE >= 5 &
        LANGUAGE != 12 & LANGUAGE != 1,
      case_when(
        SPEAKENG == 4 ~ 1,
        SPEAKENG %in% c(1,5,6) ~ 2
      ),
      NA
    ),
    labforce = ifelse(
      AGE >= 16 &
        EMPSTATD != 14 &
        EMPSTATD != 15,
      case_when(
        EMPSTATD %in% c(10,12,20) ~ 1
        EMPSTATD == 30 ~ 0
      ),
      NA
    ),
    employ = ifelse(
      AGE >= 16 & 
        EMPSTATD %in% c(10,12,20),
      case_when(
        EMPSTATD %in% c(10,12) ~ 1,
        EMPSTATD == 20 ~ 2
      ),
      NA
    ),
    selfemp = ifelse(
      AGE >= 16 & 
        EMPSTATD %in% c(10,12),
      case_when(
        CLASSWKR == 1 ~ 1,
        CLASSWKR == 2 ~ 0
      ),
      NA
    ),
    occ_group = ifelse(
      AGE >= 16 & 
        EMPSTATD %in% c(10,12) &
        OCC > 0 &
        OCC < 9800,
      case_when(
        OCC >= 10 & OCC <= 960 ~ 1
        OCC >= 1005 & OCC <= 1980 ~ 2
        OCC >= 2001 & OCC <= 2920 ~ 3
        OCC >= 3000 & OCC <= 3550 ~ 4
        OCC >= 3601 & OCC <= 4655 ~ 5
        OCC >= 4700 & OCC <= 4965 ~ 6
        OCC >= 5000 & OCC <= 5940 ~ 7
        OCC >= 6005 & OCC <= 6130 ~ 8
        OCC >= 6200 & OCC <= 6950 ~ 9
        OCC >= 7000 & OCC <= 7640 ~ 10
        OCC >= 7700 & OCC <= 8990 ~ 11
        OCC >= 9005 & OCC <= 9760 ~ 12
      ),
      NA
    ),
    ind_group = ifelse(
      AGE >= 16 & 
        EMPSTATD %in% c(10,12) &
        IND < 9370,
      case_when(
        IND >= 170 & IND <= 490 ~ 1
        IND == 770 ~ 2
        IND >= 1070 & IND <= 3990 ~ 3
        IND >= 4070 & IND <= 4590 ~ 4
        IND >= 4670 & IND <= 5790 ~ 5
        (IND >= 6070 & IND <= 6390) | (IND >= 570 & IND <= 690) ~ 6
        IND >= 6470 & IND <= 6780 ~ 7
        IND >= 6870 & IND <= 7190 ~ 8
        IND >= 7270 & IND <= 7790 ~ 9
        IND >= 7860 & IND <= 8470 ~ 10
        IND >= 8561 & IND <= 8690 ~ 11
        IND >= 8770 & IND <= 9290 ~ 12
        IND >= 9370 & IND <= 9590 ~ 13
      ),
      NA
    ),
    wage = ifelse(
      AGE >= 16 &
        INCWAGE > 0 & INCWAGE < 999998 &
        WKSWORK1 > 0 & UHRSWORK > 0,
      INCWAGE/(WKSWORK1*UHRSWORK),
      NA
    ),
    wage_15 = ifelse(
      AGE >= 16 &
        INCWAGE > 0 & INCWAGE < 999998 &
        WKSWORK1 > 0 & UHRSWORK > 0,
      case_when(
        wage >= 15 ~ 1,
        TRUE ~ 0
      ),
      NA
    ),
    hhincome = ifelse(
      PERNUM == 1 &
        GQ %in% c(1,2,5),
      case_when(
        HHINCOME == 9999999 ~ NA,
        HHINCOME < 9999999 ~ HHINCOME
      ),
      NA
    ),
    poverty = ifelse(
      GQ %in% c(1,2,5) &
        POVERTY > 0,
      case_when(
        POVERTY < 100 ~ 1,
        POVERTY >= 100 & POVERTY < 200 ~ 2,
        TRUE ~ 3
      ),
      NA
    ),
    poverty_child = ifelse(
      GQ %in% c(1,2,5) &
        POVERTY > 0 &
        AGE < 18,
      case_when(
        POVERTY < 100 ~ 1,
        POVERTY >= 100 & POVERTY < 200 ~ 2,
        TRUE ~ 3
      ),
      NA
    ),
    poverty_employed = ifelse(
      GQ %in% c(1,2,5) &
        POVERTY > 0 &
        AGE >= 16 &
        (EMPSTATD == 10 | EMPSTATD == 12),
      case_when(
        POVERTY > 100 ~ 1,
        TRUE ~ 0
      ),
      NA
    ),
    foodstamp = ifelse(
      GQ %in% c(1,2,5),
      case_when(
        FOODSTMP == 2 ~ 1,
        FOODSTMP == 1 ~ 0
      ),
      NA
    ),
    foodstamp_child = ifelse(
      GQ %in% c(1,2,5) &
        AGE < 18,
      case_when(
        FOODSTMP == 2 ~ 1,
        FOODSTMP == 1 ~ 0
      ),
      NA
    ),
    cash = ifelse(
      GQ %in% c(1,2,5) &
        AGE >= 15 &
        INCWELFR < 99999,
      case_when(
        INCWELFR > 0 ~ 1,
        INCWELFR == 0 ~ 0
      ),
      NA
    ),
    ownership = ifelse(
      GQ %in% c(1,2,5),
      case_when(
        OWNERSHIP == 1 ~ 1,
        OWNERSHIP == 2 ~ 0
      ),
      NA
    ),
    ownership_child = ifelse(
      GQ %in% c(1,2,5) &
        AGE < 18,
      case_when(
        OWNERSHIP == 1 ~ 1,
        OWNERSHIP == 2 ~ 0
      ),
      NA
    ),
    hvalue = ifelse(
      PERNUM == 1 &
        OWNERSHIP == 1 &
        GQ %in% c(1,2,5),
      case_when(
        VALUEH < 9999999 ~ VALUEH,
        VALUEH == 9999999 ~ NA, 
      ),
      NA
    ),
    owner_hcost = ifelse(
      OWNERSHIP == 1 & 
        GQ %in% c(1,2,5) & 
        OWNCOST < 99999,
      case_when(
        OWNCOST/(hhincome/12) >= .3 & OWNCOST/(hhincome/12) < .5 ~ 1,
        OWNCOST/(hhincome/12) >= .5 ~ 2,
        TRUE ~ 3
      ),
      NA
    ),
    owner_hcost_child = ifelse(
      OWNERSHIP == 1 & 
        GQ %in% c(1,2,5) & 
        OWNCOST < 99999 &
        AGE < 18,
      case_when(
        OWNCOST/(hhincome/12) >= .3 & OWNCOST/(hhincome/12) < .5 ~ 1,
        OWNCOST/(hhincome/12) >= .5 ~ 2,
        TRUE ~ 3
      ),
      NA
    ),
    renter_hcost = ifelse(
      OWNERSHIP == 2 & 
        GQ %in% c(1,2,5) & 
        RENTGRS > 0,
      case_when(
        RENTGRS/(hhincome/12) >= .3 & RENTGRS/(hhincome/12) < .5 ~ 1,
        RENTGRS/(hhincome/12) >= .5 ~ 2,
        TRUE ~ 3
      ),
      NA
    ),
    renter_hcost_child = ifelse(
      OWNERSHIP == 2 & 
        GQ %in% c(1,2,5) & 
        RENTGRS > 0 &
        AGE < 18,
      case_when(
        RENTGRS/(hhincome/12) >= .3 & RENTGRS/(hhincome/12) < .5 ~ 1,
        RENTGRS/(hhincome/12) >= .5 ~ 2,
        TRUE ~ 3
      ),
      NA
    ),
    overcrowded = ifelse(
      GQ %in% c(1,2,5), 
      case_when(
        NUMPREC/ROOMS > 1 ~ 1,
        TRUE ~ 0
      ),
      NA
    ),
    overcrowded_child = ifelse(
      GQ %in% c(1,2,5) &
        AGE < 18, 
      case_when(
        NUMPREC/ROOMS > 1 ~ 1,
        TRUE ~ 0
      ),
      NA
    ),
    internet = ifelse(
      GQ %in% c(1,2,5),
      case_when(
        CINETHH == 3 ~ 1,
        CINETHH == 1 | CINETHH == 2 ~ 0
      ),
      NA
    ),
    broadband = ifelse(
      GQ %in% c(1,2,5),
      case_when(
        CIHISPEED == 20 ~ 1,
        CIHISPEED == 10 ~ 0
      ),
      NA
    ),
    laptop = ifelse(
      GQ %in% c(1,2,5),
      case_when(
        CILAPTOP == 2 ~ 1,
        CILAPTOP == 1 ~ 0
      ),
      NA
    ),
    smartphone = ifelse(
      GQ %in% c(1,2,5),
      case_when(
        CISMRTPHN == 2 ~ 1,
        CISMRTPHN == 1 ~ 0
      ),
      NA
    ),
    tablet = ifelse(
      GQ %in% c(1,2,5),
      case_when(
        CITABLET == 2 ~ 1,
        CITABLET == 1 ~ 0
      ),
      NA
    ),
    device = ifelse(
      GQ %in% c(1,2,5),
      case_when(
        CILAPTOP == 2 & CISMRTPHN == 2 & CITABLET == 2 ~ 1,
        TRUE ~ 0
      ),
      NA
    ),
    vehicle = ifelse(
      GQ %in% c(1,2,5),
      case_when(
        VEHICLES == 9 ~ 1,
        VEHICLES > 0 & VEHICLES < 9 ~ 0
      ),
      NA
    ),
    wfh = ifelse(
      AGE >= 16,
      GQ %in% c(1,2,5),
      WRKLSTWK == 2,
      case_when(
        TRANWORK == 0 ~ 1,
        TRUE ~ 0
      ),
      NA
    ),
    commute_time = ifelse(
      AGE >= 16 &
        GQ %in% c(1,2,5) &
        WRKLSTWK == 2 &
        TRANWORK > 0,
      TRANTIME,
      NA
    ),
    public_commute = ifelse(
      AGE >= 16 &
        GQ %in% c(1,2,5) &
        WRKLSTWK == 2 &
        TRANWORK > 0,
      case_when(
        TRANWORK %in% c(31, 34, 36, 37, 39) ~ 1,
        TRUE ~ 0
      ),
      NA
    ),
    coverage_type = case_when(
      HCOVANY == 1 ~ 1,
      HCOVANY == 2 & HINSCARE == 2 ~ 2,
      HCOVANY == 2 & HINSCAID == 2 & HINSCARE == 1 ~ 3,
      HCOVANY == 2 & HINSVA == 2 & HINSCAID == 1 & HINSCARE == 1 ~ 4,
      HCOVANY == 2 & HINSEMP == 2 & HINSVA == 1 & HINSCAID == 1 & HINSCARE == 1 ~ 5,
      HCOVANY == 2 & HINSPUR == 2 & HINSEMP == 1 & HINSVA == 1 & HINSCAID == 1 & HINSCARE == 1 ~ 6,
      HCOVANY == 2 & HINSPUR == 1 & HINSEMP == 1 & HINSVA == 1 & HINSCAID == 1 & HINSCARE == 1 ~ 7
    ),
    medicaid_child = ifelse(
      AGE < 18,
      case_when(
        HCOVANY == 2 & HINSCAID == 2 & HINSCARE == 1 ~ 1,
        TRUE ~ 0
      ),
      NA
    ),
    uninsured_child = ifelse(
      AGE < 18,
      case_when(
        HCOVANY == 1 ~ 1,
        TRUE ~ 0
      ),
      NA
    ),
    eligible_voters = case_when(
      CITIZEN >= 0 & CITIZEN <= 2 & AGE >= 18 ~ 1,
      CITIZEN > 2 | AGE < 18 ~ 0
    ))


# Take a look at the processed data summary to ensure
# computed variables look reasonable
processed %>% summary()
processed %>% str()
