package com.tlcn.order_service.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.serializer.GenericToStringSerializer;
import org.springframework.data.redis.serializer.Jackson2JsonRedisSerializer; 
import org.springframework.data.redis.serializer.StringRedisSerializer;

@Configuration
public class RedisConfig {

    /**
     * Bean DÀNH CHO CART SERVICE (Lưu trữ đối tượng Cart).
     * @param connectionFactory
     * @return RedisTemplate<String, Object> sử dụng Jackson cho serialization.
     */
    @Bean
    public RedisTemplate<String, Object> cartRedisTemplate(RedisConnectionFactory connectionFactory) {
        RedisTemplate<String, Object> template = new RedisTemplate<>();
        template.setConnectionFactory(connectionFactory);
        
        // Key Serializer: String
        template.setKeySerializer(new StringRedisSerializer());
        template.setHashKeySerializer(new StringRedisSerializer());
        
        // Value Serializer: Dùng Jackson để serialize Cart sang JSON
        // LỖI ĐÃ ĐƯỢC SỬA TẠI ĐÂY:
        Jackson2JsonRedisSerializer<Object> jsonSerializer = new Jackson2JsonRedisSerializer<>(Object.class);
        
        template.setValueSerializer(jsonSerializer);
        template.setHashValueSerializer(jsonSerializer);

        template.afterPropertiesSet();
        return template;
    }

    /**
     * Định nghĩa bean RedisTemplate<String, Integer> để sử dụng cho Stock Caching.
     * @param connectionFactory Được inject tự động bởi Spring Boot.
     * @return RedisTemplate tùy chỉnh.
     */
    @Bean
    public RedisTemplate<String, Integer> stockRedisTemplate(RedisConnectionFactory connectionFactory) {        RedisTemplate<String, Integer> template = new RedisTemplate<>();
        template.setConnectionFactory(connectionFactory);
        
        // Key Serializer: Lưu trữ key dưới dạng String
        template.setKeySerializer(new StringRedisSerializer());
        template.setHashKeySerializer(new StringRedisSerializer());
        
        // Value Serializer: Lưu trữ value (Integer) dưới dạng String
        template.setValueSerializer(new GenericToStringSerializer<>(Integer.class));
        template.setHashValueSerializer(new GenericToStringSerializer<>(Integer.class));

        template.afterPropertiesSet();
        return template;
    }
}