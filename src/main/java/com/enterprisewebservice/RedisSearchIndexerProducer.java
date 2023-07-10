package com.enterprisewebservice;

import jakarta.enterprise.inject.Produces;
import redis.clients.jedis.JedisPooled;
import jakarta.enterprise.context.ApplicationScoped;
import org.eclipse.microprofile.config.inject.ConfigProperty;

@ApplicationScoped
public class RedisSearchIndexerProducer {

    @ConfigProperty(name = "redis.host")
    String redisHost;

    @Produces
    public RedisSearchIndexer createRedisSearchIndexer() {
        return new RedisSearchIndexer(new JedisPooled(redisHost, 6379));
    }
}
