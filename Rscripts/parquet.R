require(arrow)
require(tidyverse)

setwd("C:/code/forumlocalranking")

genders <- read_csv("data/gender_clean.csv")

users <- read_parquet("user_profiles.parquet") %>%
  mutate(posts=abs(posts)) %>%
  left_join(genders, by=c("gender")) %>%
  select(name, posts, rating, gender=gender_clean)
top_users <- users %>%
  filter(posts > 0 & rating > 0)

user_ratings <- read_parquet("user_ratings.parquet")
user_ratings <- user_ratings %>%
  filter(total > 2 & rater %in% top_users$name & ratee %in% top_users$name) %>%
  mutate(name=ratee, pos=(sum+total)/2, neg=(total-sum)/2) %>%
  left_join(users, by=c("name")) %>%
  mutate(dpos=as.numeric(pos)/posts, dneg=as.numeric(neg)/posts) %>%
  group_by(rater) %>%
  mutate(spos=scale(dpos, center=0), sneg=scale(dneg, center=0)) %>%
  mutate(ssum=spos-sneg)

user_ratings_short <- user_ratings %>%
  select(rater, ratee, score=ssum, pos, neg) %>%
  left_join(users, by=c("rater"="name")) %>%
  select(-posts, -rating, rater_g=gender) %>%
  left_join(users, by=c("ratee"="name")) %>%
  select(-posts, -rating, ratee_g=gender) %>%
  select(rater, rater_g, ratee_g, ratee, score, pos, neg)

user_x <- user_ratings_short %>%
  left_join(user_ratings_short, by=c("ratee"="rater", "rater"="ratee")) %>%
  select(rater, fneg=neg.x, fpos=pos.x, fwd=score.x, fg=rater_g.x, rg=ratee_g.x, rev=score.y, rpos=pos.y, rneg=neg.y, ratee)

user_scores %>%
  mutate(fwd=pmin(1, pmax(-1, fwd))) %>%
  group_by(ratee) %>%
  filter(n() > 10) %>%
  summarize(score=mean(fwd)) %>%
  ungroup() %>%
  left_join(users, by=c("ratee"="name")) %>%
  mutate(rating=as.numeric(rating)) %>%
  mutate(avgpostrating=rating/posts) %>%
  select(gender, name=ratee, score, avgpostrating, rating, posts) %>%
  arrange(desc(score)) %>%
  View

# koturn
# Pastushok / JordanTwoDelta
# Maestro / ethery
user_x %>%
  filter(ratee == "alepar") %>%
  View

user_x %>%
  filter(rater == "PorcelainDoll") %>%
  View

user_x %>%
  filter(rater == "Sweet_Lilu") %>%
  View #!!! CROTishka

user_x %>%
  filter(rater == "SNezhny_Max") %>%
  View #!!! CROTishka

user_x %>%
  filter(rater == "Nereida") %>%
  View #!!! Watrox

user_x %>%
  filter(rater == "ManMachine") %>%
  View

user_x %>%
  filter(rater == "SunnyCat") %>%
  View

user_x %>%
  filter(rater == "mr_knyaz") %>%
  View

user_x %>%
  filter(rater == "Riamella") %>%
  View

user_x %>%
  filter(ratee == "Electra") %>%
  View # !!! secret/indiffirent lovers

user_ratings %>%
  filter(rater == "Raccoon") %>%
  View

user_x %>%
  filter(rater == "Sardaukar") %>%
  View

user_ratings %>%
  filter(rater == "parampam") %>%
  View

user_ratings %>%
  filter(rater == "botWi") %>%
  View

user_ratings %>%
  filter(ratee == "patnic") %>%
  View

user_ratings %>%
  filter(rater == "DeeMon") %>%
  View

user_ratings %>%
  filter(rater == "Blade_runner") %>%
  View
user_ratings %>%
  filter(ratee == "Blade_runner") %>%
  View

user_ratings %>%
  filter(rater == "Paloma_Blanca") %>%
  View
user_ratings %>%
  filter(ratee == "Paloma_Blanca") %>%
  View

user_x %>%
  filter(rater == "MardJane") %>%
  View

user_x %>%
  filter(rater == "piton") %>%
  View # !!! trusixa

user_x %>%
  filter(rater == "Midget") %>%
  View

user_x %>%
  filter(rater == "Violet") %>%
  View # !!! Raccoon mutual dislike

user_x %>%
  filter(rater == "Ginger_Fox") %>%
  View

user_x %>%
  filter(rater == "earl") %>%
  View

user_x %>%
  filter(rater == "pianist") %>%
  View

user_x %>%
  filter(rater == "AlexSa") %>%
  View
