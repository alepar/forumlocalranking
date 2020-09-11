### features
* log window
* join user_gender_expert.csv into parquet data
* calculate user intersection (topics of each other they've potentially seen)
  - determine in which boards they were active. use that as the intersection size
  - fetch all <user, topic_id, post_id, date, rating>
  - calculate all topics both users been present in. use that as the intersection size
  - determine which boards at which time intervals they were active. use that as the intersection size.
  
### ideas
* parquet instead of lmdb?
    * another storage engine? mdbx?
* cache pages instead of parsed data?
* store lists of posts and do a smart resume (list of posts can be changed though - moderators, deletes, etc)
    - make sure you can regenerate links back into forum
* two users:
    - times (years/months) of intersecting activity
    - boards with intersecting activity
    - threads with intersecing activity
* highly rated pics
* top holywar topics
* behaviors of top users?
* ratings normalized not by posts count but by intersection time?
* look at cases where my rating is very different from their rating
* anomalous threads 
    - super active (high frequency posting)
    - anomalous ratings
    - doc2vec?
* save posts text/content -> NLP/GPT?
* model that predicts rating
