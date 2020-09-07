library(tidyverse)
library(ggplot2)
library(plotly)

data_path <- "/Users/parfenov/code/forumlocalranking"
setwd(data_path)

user_ratings_byuser <- read_csv("user_ratings_byuser.csv")
user_details <- read_csv("user_details.csv")
gender_clean <- read_csv("data/gender_clean.csv")
user_gender_expert <- read_csv("data/user_gender_expert.csv")

user_details <- user_details %>%
    left_join(gender_clean %>% select(-count), by=c("gender")) %>%
    select(-gender) %>%
    left_join(user_gender_expert, by=c("user")) %>%
    mutate(gender=ifelse(is.na(gender_clean), gender_expert, gender_clean)) %>%
    select(-gender_clean, -gender_expert)


user_ratings_byuser_gender <- user_ratings_byuser %>%
    mutate(pos=(tot+sum)/2, neg=(tot-sum)/2, rpos=(rtot+rsum)/2, rneg=(rtot-rsum)/2) %>%
    left_join(
        user_details %>% select(user, gender) %>% rename(rater_gender=gender), 
        by=c("rater" = "user")
    ) %>%
    left_join(
        user_details %>% select(user, gender) %>% rename(ratee_gender=gender), 
        by=c("ratee" = "user")
    ) %>%
    mutate(gender_pair=ifelse(is.na(rater_gender) | is.na(ratee_gender), NA, paste0(rater_gender, "->", ratee_gender)))

user_ratings_byuser_gender <- user_ratings_byuser_gender %>%
    filter(!is.na(rater_gender), !is.na(ratee_gender))

user_pos_neg_tot <- user_ratings_byuser_gender %>%
    group_by(rater) %>%
    summarise(possum=sum(pos), negsum=sum(neg), tot=possum+negsum)

user_pos_neg_bygender <- user_ratings_byuser_gender %>%
    group_by(rater, rater_gender, ratee_gender) %>%
    summarise(possum=sum(pos), negsum=sum(neg)) %>%
    left_join(user_pos_neg_tot, by=c("rater"), suffix=c("", "_totals"))

# EXPERIMENTAL below

gathered <- user_pos_neg_bygender %>%
    filter(!is.na(rater_gender), !is.na(ratee_gender)) %>%
    filter(possum_totals > 0, negsum_totals > 0) %>%
#    filter(tot > 100) %>%
    mutate(possum_ratio = possum/possum_totals, negsum_ratio = negsum/negsum_totals) %>%
    select(rater, rater_gender, ratee_gender, possum_ratio, negsum_ratio)

posratios <- gathered %>%
    select(-negsum_ratio) %>%
    spread(ratee_gender, possum_ratio) %>%
    rename(pos_ratio_M=M, pos_ratio_F=F)

negratios <- gathered %>%
    select(-possum_ratio) %>%
    spread(ratee_gender, negsum_ratio) %>%
    rename(neg_ratio_M=M, neg_ratio_F=F)

ratios <- posratios %>%
    left_join(negratios, by=c("rater", "rater_gender")) %>%
    filter(neg_ratio_F>0, pos_ratio_F>0) %>%
    mutate(ratio_F=pos_ratio_F/neg_ratio_F, ratio_M=pos_ratio_M/pos_ratio_F) %>%
    select(-starts_with("pos"), -starts_with("neg")) %>%
    rename(M=ratio_M, F=ratio_F) %>%
    gather(ratee_gender, ratio, -rater, -rater_gender)

#!!!! double check !!!!
ratios %>% 
    group_by(rater_gender, ratee_gender) %>%
    summarise(avgratio=mean(ratio))

(
user_ratings_byuser_gender %>%    
    filter(tot > 0) %>%
#    filter(sum > -100, sum < 100) %>%
    mutate(sig = 1 / (1+exp( -sum/ sqrt(abs(sum)) / 10) ) ) %>%
ggplot(aes(x=sig, color=gender_pair)) +
    geom_freqpoly(binwidth=0.025)
) %>% ggplotly




user_totsum <- user_ratings_byuser_gender %>%
    group_by(rater) %>%
    summarise(totcum=sum(tot))


    
(
    user_ratings_byuser_gender %>%
        left_join(user_totsum, by=c("rater")) %>%
        group_by(rater, gender_pair) %>%
        summarise(sumn=sum(tot/totcum)) %>% View
    ggplot(aes(x=sumn, color=gender_pair)) +
        geom_freqpoly(bins=100)
) %>% ggplotly
