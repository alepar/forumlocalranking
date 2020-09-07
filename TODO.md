* join user_gender_expert.csv into parquet data
* calculate user intersection (topics of each other they've potentially seen)
  - determine in which boards they were active. use that as the intersection size
  - fetch all <user, topic_id, post_id, date, rating>
    + kotlin?
    + another storage engine? mdbx?
    + another pipelining? rxjava? reactive streams?
    + cache pages instead of parsed data?
    + store lists of posts and do a smart resume (list of posts can be changed though - moderators, deletes, etc)
  - calculate all topics both users been present in. use that as the intersection size
  - determine which boards at which time intervals they were active. use that as the intersection size. 