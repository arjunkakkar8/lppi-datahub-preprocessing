library(stringr)
library(googlesheets4)

## Use for styling file based on tidyverse style guide
## Keeps the file manageable and readable
# library(styler)
# styler:::style_active_file()

## While editing large datasets, there may be a need
## to increase the memory limit. Use the lines below
## to edit the .REnviron file and add: R_MAX_VSIZE=100Gb
# library(usethis)
# usethis::edit_r_environ()


## Testing code to ensure that county coding is correct
## and that the function is executing properly
# test <- raw %>%
#   slice(1:10000000) %>%
#   append_county_codes()
# 
# test <- raw %>%
#   append_county_codes()
# 
# a <- processed %>%
#   group_by(SERIAL) %>%
#   summarize(members = mean(NUMPREC), count = n()) %>%
#   mutate(check = members == count)
# 
# test %>% filter(SERIAL == '1721774') %>% View

append_county_codes <- function(raw,
                                puma_var = "PUMA",
                                statefip_var = "STATEFIP",
                                county_codes_raw) {
  # Download county codes is dataframe is not provided
  if (missing(county_codes_raw)) {
    message("No county codes provided, loading codes from the google sheet")
    gs4_deauth()
    county_codes_raw <-
      read_sheet(
        "1TjwmqQWqW7u_aDzf-2xzlT12jo_gUU_PQeddmbGFF0U",
        sheet = 2,
        col_types = "c"
      )
  }
  
  # Create a list of possible pumas
  pumas <- raw %>%
    group_by(!!sym(statefip_var)) %>%
    summarise(!!sym(puma_var) := unique(!!sym(puma_var)) %>% as.integer()) %>%
    ungroup()
  
  # Create a dataframe that can be used to perform the merge
  county_codes <- 1:nrow(county_codes_raw) %>%
    # Loop over all rows and split up the puma groups
    # into singles and ranges
    lapply(function(index) {
      strsplit(county_codes_raw[index,]$puma_range, ",")[[1]] %>%
        # Handle each puma term
        lapply(function(puma_term) {
          term <- str_trim(puma_term)
          
          if (grepl("-", term)) {
            # If the term is a range loop over all possible
            # puma values and create rows for each and collect
            # them in one dataframe
            bounds <- strsplit(term, "-")[[1]] %>% as.integer()
            pumas %>%
              filter(
                !!sym(statefip_var) == as.integer(county_codes_raw[index,]$statefip),!!sym(puma_var) >= bounds[1],!!sym(puma_var) <= bounds[2]
              ) %>%
              pull(!!sym(puma_var)) %>%
              lapply(function(puma_term) {
                county_codes_raw[index,] %>%
                  mutate(puma = puma_term) %>%
                  return()
              }) %>%
              bind_rows()
          } else {
            # If the term is a single, just create one row
            # for that term
            county_codes_raw[index,] %>%
              mutate(puma = as.integer(puma_term)) %>%
              return()
          }
        }) %>% bind_rows()
    }) %>%
    bind_rows() %>%
    # Remove unused variables
    select(-puma_range,-state) %>%
    # Ensure that statefip is an integer
    mutate(statefip = as.integer(statefip))
  
  merge_on <- c("puma", "statefip")
  names(merge_on) <- c(puma_var, statefip_var)
  
  raw %>%
    left_join(county_codes, by = merge_on) %>%
    return()
}
