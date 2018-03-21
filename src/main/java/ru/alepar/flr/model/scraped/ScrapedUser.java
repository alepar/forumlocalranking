package ru.alepar.flr.model.scraped;

public class ScrapedUser {

    private String name;

    private ScrapedUserPostsStats postsStats;
    private ScrapedUserRatings ratings;
    private ScrapedUserDetails details;

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public ScrapedUserPostsStats getPostsStats() {
        return postsStats;
    }

    public ScrapedUserRatings getRatings() {
        return ratings;
    }

    public void setPostsStats(ScrapedUserPostsStats postsStats) {
        this.postsStats = postsStats;
    }

    public void setRatings(ScrapedUserRatings ratings) {
        this.ratings = ratings;
    }

    public ScrapedUserDetails getDetails() {
        return details;
    }

    public void setDetails(ScrapedUserDetails details) {
        this.details = details;
    }
}
