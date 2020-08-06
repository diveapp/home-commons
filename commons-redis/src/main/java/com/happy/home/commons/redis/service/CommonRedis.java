package com.happy.home.commons.redis.service;

import com.fasterxml.jackson.core.type.TypeReference;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.happy.home.commons.lang.utils.JsonUtil;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.connection.RedisConnection;
import org.springframework.data.redis.core.RedisOperations;
import org.springframework.data.redis.core.SetOperations;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.serializer.RedisSerializer;
import org.springframework.stereotype.Component;
import org.springframework.util.Assert;
import org.springframework.util.CollectionUtils;
import org.springframework.util.ObjectUtils;
import org.springframework.util.StringUtils;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.annotation.PostConstruct;
import javax.annotation.Resource;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;

import static java.util.Objects.isNull;

/**
 * @author: lijixiao
 * @date: 2020-08-05
 */
@Slf4j
@Component
public class CommonRedis {

    private static StringRedisTemplate template;

    private static SetOperations<String, String> sOps;

    @Resource
    private StringRedisTemplate redisTemplate;

    @PostConstruct
    public void init() {
        log.info("[REDIS] init begin");
        CommonRedis.template = redisTemplate;
        sOps = redisTemplate.opsForSet();
        log.info("[REDIS] init end");
    }

    /**
     * 加锁前先获得该key的ttl
     */
    public static boolean lock(@Nonnull String key, @Nonnull String requestId) {
        log.info("[REDIS] lock key: {}, requestId: {}", key, requestId);
        Boolean success = false;
        Long ttl = template.getExpire(key);
        //ttl为-1表示永久有效，ttl为-2表示key不存在，这两种情况都需要去尝试获得锁
        if (ttl != null && ttl < 0) {
            success = template.opsForValue().setIfAbsent(key, requestId);
            if (ttl == -1L || (success != null && success)) {
                template.expire(key, 10, TimeUnit.SECONDS);
            }
        }
        return success == null ? false : success;
    }

    /**
     * requestId用做解锁标识，哪个请求加的锁就由哪个请求解锁
     */
    public static boolean unlock(@Nonnull String key, @Nonnull String requestId) {
        log.info("[REDIS] unlock key: {}, requestId: {}", key, requestId);
        template.delete(key);
        return true;
    }

    /**
     * 从redis中查询对应的key并序列化为指定的对象，如果该key不存在或者值为NULL，则方法会返回null
     */
    @Nullable
    public static <T> T get(@Nonnull String key, Class<T> type) {
        log.info("[REDIS] get Class value, key: {}", key);
        String json = template.opsForValue().get(key);
        if (isNull(json)) {
            return null;
        }
        return JsonUtil.readStringAsObject(json, type);
    }

    /**
     * 从redis中查询对应的key并序列化为指定的泛型类型对象，如果该key不存在或者值为NULL，则方法会返回null
     */
    public static <T> T get(@Nonnull String key, TypeReference<T> typeReference) {
        log.info("[REDIS] get TypeReference value, key: {}", key);
        String json = template.opsForValue().get(key);
        if (isNull(json)) {
            return null;
        }
        return JsonUtil.readStringAsObject(json, typeReference);
    }

    /**
     * 将对象序列化为json字符串并存入redis，失效时间为1小时
     */
    public static void put(@Nonnull String key, @Nonnull Object value) {
        log.info("[REDIS] put key: {}, value: {}", key, value);
        template.opsForValue().set(key, JsonUtil.writeObjectAsString(value), 1, TimeUnit.HOURS);
    }

    /**
     * 将对象序列化为json字符串并存入redis，失效时间由调用者指定
     */
    public static void put(@Nonnull String key, @Nonnull Object value, TimeUnit timeUnit, long timeout) {
        log.info("[REDIS] put key: {}, value: {}, timeout:{}, timeUnit: {}", key, value, timeout, timeUnit);
        template.opsForValue().set(key, JsonUtil.writeObjectAsString(value), timeout, timeUnit);
    }

    /**
     * 批量清除keys
     */
    public static void deleteKeys(String... keys) {
        log.info("[REDIS] deleteKeys： {}", JsonUtil.writeObjectAsString(keys));
        List<String> ks = Lists.newArrayList(keys);
        template.delete(ks);
    }

    /**
     * 从redis中获取指定key的定长队列
     */
    public static <T> List<T> getFixedLengthList(@Nonnull String key, Integer length, TypeReference<T> typeReference) {
        log.info("[REDIS] getFixedLengthList key: {}, length: {}, typeReference: {}", key, length, typeReference);
        List<T> resultList = new ArrayList<>();
        Boolean exist = template.opsForList().getOperations().hasKey(key);
        if (exist != null && exist) {
            List<String> list = template.opsForList().range(key, 0, length);
            if (null != list){
                list.forEach(str -> resultList.add(JsonUtil.readStringAsObject(str, typeReference)));
                return resultList;
            }
        }
        return resultList;
    }

    /**
     * 增加redis定长队列元素
     */
    public static <T> void addFixedLengthList(String key, Integer length, List<T> list) {
        log.info("[REDIS] addFixedLengthList key: {}, length: {}", key, length);
        if (!CollectionUtils.isEmpty(list)) {
            List<String> jsonStringList = new ArrayList<>(list.size());
            list.forEach(t -> jsonStringList.add(JsonUtil.writeObjectAsString(t)));
            template.opsForList().leftPushAll(key, jsonStringList);
            template.opsForList().trim(key, 0, length);
        }
    }

    /**
     * 获取redis队列指定位置的元素
     */
    public static <T> T getOneInFixedLengthList(String key, Integer index, TypeReference<T> typeReference) {
        log.info("[REDIS] getOneInFixedLengthList key: {}, index: {}", key, index);
        String jsonStr = template.opsForList().index(key, index);
        if (!StringUtils.isEmpty(jsonStr)) {
            return JsonUtil.readStringAsObject(jsonStr, typeReference);
        }
        return null;
    }

    /**
     * 修改redis队列中的元素
     */
    public static void replaceOneInFixedLengthList(String key, Integer index, Object object) {
        log.info("[REDIS] replaceOneInFixedLengthList key: {}, index: {}", key, index);
        if (!StringUtils.isEmpty(key) && !ObjectUtils.isEmpty(object)) {
            template.opsForList().set(key, index, JsonUtil.writeObjectAsString(object));
        }
    }

    /**
     * 移除redis队列中的元素
     */
    public static void removeOneInFixedLengthList(String key, long count, String value) {
        log.info("[REDIS] removeOneInFixedLengthList key: {}, count: {}, value: {}", key, count, value);
        template.opsForList().remove(key, count, value);
    }

    public static void setPush(String key, Object value) {
        log.info("[REDIS] setPush key: {}, value: {}", key, value);
        sOps.add(key, JsonUtil.writeObjectAsString(value));
    }

    public static <T> void setPushMulti(String key, Set<T> values) {
        log.info("[REDIS] setPushMulti key: {}, values: {}", key, values);

        String[] vs = values.stream()
                .map(JsonUtil::writeObjectAsString)
                .toArray(String[]::new);

        sOps.add(key, vs);
    }

    public static void setRemove(String key, Object value) {
        log.info("[REDIS] setRemove key: {}, values: {}", key, value);
        sOps.remove(key, JsonUtil.writeObjectAsString(value));
    }

    public static <T> Set<T> setListAll(String key, Class<T> c) {
        log.info("[REDIS] setListAll key: {}, c: {}", key, c);
        Long len = sOps.size(key);
        if (len != null) {
            Set<String> vs = sOps.members(key);
            if (vs != null){
                return vs.stream().map(s -> JsonUtil.readStringAsObject(s, c)).collect(Collectors.toSet());
            }
        }
        return Sets.newHashSet();
    }

    public static Boolean setContains(String key, Object value) {
        log.info("[REDIS] setContains key: {}, value: {}", key, value);
        return sOps.isMember(key, JsonUtil.writeObjectAsString(value));
    }

    public static Long setCount(String key) {
        return sOps.size(key);
    }

    public static boolean hasKey(String key) {
        log.info("[REDIS] hasKey key: {}", key);
        Boolean exist =  template.hasKey(key);
        return exist == null ? false : exist;
    }

    public static boolean expire(String key, long timeout) {
        log.info("[REDIS] expire key: {}, timeout: {}", key, timeout);
        Boolean result = template.expire(key, timeout, TimeUnit.SECONDS);
        return result == null ? false : result;
    }

    /**
     * 序列化 key
     *
     * @param key
     * @return
     */
    private static byte[] rawKey(Object key) {
        Assert.notNull(key, "non null key required");

        log.info("[REDIS] rawKey key: {}", key);
        RedisSerializer serializer = template.getKeySerializer();

        return serializer.serialize(key);
    }

    private static byte[][] rawValues(Collection<Object> values) {
        Assert.notEmpty(values, "Values must not be 'null' or empty.");
        Assert.noNullElements(values.toArray(), "Values must not contain 'null' value.");

        byte[][] rawValues = new byte[values.size()][];
        int i = 0;
        for (Object value : values) {
            rawValues[i++] = rawValue(value);
        }

        return rawValues;
    }

    private static byte[] rawValue(Object value) {
        log.info("[REDIS] rawValue value: {}", value);
        RedisSerializer serializer = template.getValueSerializer();

        return serializer.serialize(value);
    }
}
