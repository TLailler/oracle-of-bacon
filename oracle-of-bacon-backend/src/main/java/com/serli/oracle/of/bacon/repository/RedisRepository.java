package com.serli.oracle.of.bacon.repository;

import redis.clients.jedis.Jedis;

import java.util.List;

public class RedisRepository {
    private final Jedis jedis;
    private static final String KEY_10_LAST_SEARCHES = "bacon10LastSearches";

    public RedisRepository() {
        this.jedis = new Jedis("localhost");
    }

    public List<String> getLastTenSearches() {
    	// la valeur -1 pour le paramètre "end" désigne la fin de la liste
        return jedis.lrange(KEY_10_LAST_SEARCHES, 0, -1);
    }
    
    public void storeLastSearch(String actorName) {
    	jedis.lpush(KEY_10_LAST_SEARCHES, actorName);
    	
    	// On garde les 10 derniers éléments de la liste
    	jedis.ltrim(KEY_10_LAST_SEARCHES, 0, 9);
    }
}
