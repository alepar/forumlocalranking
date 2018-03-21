library(tidyverse)

user_ratings_byuser <- read_csv("code/forumlocalranking/user_ratings_byuser.csv")
user_details <- read_csv("code/forumlocalranking/user_details.csv")
gender_clean <- read_csv("code/forumlocalranking/data/gender_clean.csv")
user_gender_expert <- read_csv("code/forumlocalranking/data/user_gender_expert.csv")

user_details <- user_details %>%
    left_join(gender_clean %>% select(-count), by=c("gender")) %>%
    select(-gender) %>%
    left_join(user_gender_expert, by=c("user")) %>%
    mutate(gender=ifelse(is.na(gender_clean), gender_expert, gender_clean)) %>%
    select(-gender_clean, -gender_expert)