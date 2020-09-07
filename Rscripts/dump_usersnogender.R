library(tidyverse)

user_details %>% 
    filter(posts > 500, is.na(gender), rating>100) %>% 
    arrange(desc(rating)) %>%
    write_csv("code/forumlocalranking/topusers_nogender.csv")
