library(tidyverse)

user_details <- read_csv("code/forumlocalranking/user_details.csv")

user_details %>%
    group_by(gender) %>%
    summarise(count = n()) %>%
    arrange(desc(count)) %>%
    write_csv("code/forumlocalranking/genders.csv")