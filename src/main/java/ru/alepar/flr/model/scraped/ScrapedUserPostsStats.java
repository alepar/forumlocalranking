package ru.alepar.flr.model.scraped;

import java.util.Map;

public class ScrapedUserPostsStats {

    private Map<String, Integer> postsInBoards;

    public Map<String, Integer> getPostsInBoards() {
        return postsInBoards;
    }

    public void setPostsInBoards(Map<String, Integer> postsInBoards) {
        this.postsInBoards = postsInBoards;
    }
}
