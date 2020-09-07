package ru.alepar.flr.model.scraped;

import java.util.Map;

public class ScrapedUserRatings {

    private Map<String, Integer> byBoard;
    private Map<String, Long> byUser;

    public Map<String, Integer> getByBoard() {
        return byBoard;
    }

    public void setByBoard(Map<String, Integer> byBoard) {
        this.byBoard = byBoard;
    }

    public Map<String, Long> getByUser() {
        return byUser;
    }

    public void setByUser(Map<String, Long> byUser) {
        this.byUser = byUser;
    }

    public static Long toLong(int sum, int votes) {
        return ((long) sum) << 32 | ((long)votes);
    }

    public static int getTotal(long value) {
        return (int) value;
    }

    public static int getSum(long value) {
        return (int) (value >> 32);
    }
}
