library(tidyverse)
library(tictoc)
library(srvyr)
library(robsurvey)
library(DBI)

con <- dbConnect(
  RPostgres::Postgres(),
  dbname = "postgres",
  host = 'db.xltlukmfiegifpljlhzh.supabase.co',
  port = 5432,
  user = "postgres",
  password = 'xxx'
)

sample <- read.csv('data/processed_sample.csv')
full <- read.csv('data/processed_full.csv')

tic()
sample %>%
  filter(!is.na(poverty)) %>%
  group_by(STATEFIP, poverty) %>%
  summarize(total = sum(PERWT)) %>%
  mutate(prop = total / sum(total))
toc()


tic()
full %>%
  filter(!is.na(poverty)) %>%
  group_by(STATEFIP, poverty) %>%
  summarize(total = sum(PERWT)) %>%
  mutate(prop = total / sum(total))
toc()


tic()
sample %>%
  filter(!is.na(hhincome)) %>%
  group_by(STATEFIP) %>%
  summarize(income = weighted_median(hhincome, HHWT))
toc()

tic()
full %>%
  filter(!is.na(hhincome)) %>%
  group_by(STATEFIP) %>%
  summarize(income = weighted_median(hhincome, HHWT))
toc()


tic()
sample %>%
  filter(!is.na(AGE)) %>%
  group_by(STATEFIP) %>%
  summarize(income = weighted_median(AGE, PERWT))
toc()

tic()
full %>%
  filter(!is.na(AGE)) %>%
  group_by(STATEFIP) %>%
  summarize(income = weighted_median(AGE, PERWT))
toc()

sample %>%
  select(PERWT, HHWT, hhincome, AGE, poverty, STATEFIP) %>%
  write.csv('data/processed_sample_select.csv', row.names = FALSE, quote = FALSE, na = "")



tic()
full %>%
  summarize(income = weighted_median(hhincome, HHWT, na.rm = TRUE))
full %>%
  group_by(STATEFIP) %>%
  summarize(income = weighted_median(hhincome, HHWT, na.rm = TRUE))
toc()
