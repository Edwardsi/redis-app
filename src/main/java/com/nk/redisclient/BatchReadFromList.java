package com.nk.redisclient;

import java.util.Collections;
import java.util.List;

import javax.annotation.PostConstruct;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.dao.DataAccessException;
import org.springframework.data.redis.connection.RedisConnection;
import org.springframework.data.redis.connection.ReturnType;
import org.springframework.data.redis.connection.StringRedisConnection;
import org.springframework.data.redis.core.RedisCallback;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.data.redis.core.script.RedisScript;
import org.springframework.stereotype.Component;

@Component
public class BatchReadFromList {

	Logger logger = LoggerFactory.getLogger(BatchReadFromList.class);
	
	private StringRedisTemplate redisTemplate;
	
	@Value("${redis.loadfromlist.batchsize}")
	private String batchSize;
	
	@Value("${redis.loadfromlist.waitTimeAfterEmptyInMS}")
	private String waitTime;
	private long waitTimeInMS;
	
	@PostConstruct
	public void init(){
		waitTimeInMS = Integer.parseInt(waitTime);
	}

	@Autowired
	public BatchReadFromList(StringRedisTemplate template) {
		this.redisTemplate = template;
	}
	
	public RedisScript<List> script() {
	  DefaultRedisScript<List> redisScript = new DefaultRedisScript<List>();
	  //redisScript.setScriptSource(new ResourceScriptSource(new ClassPathResource("META-INF/scripts/checkandset.lua")));
	  redisScript.setScriptText(luaScriptBatchReadFromList);
	  redisScript.setResultType(List.class);
	  return redisScript;
	}
	
	private final String luaScriptBatchReadFromList = "local batchsize = tonumber(ARGV[1])\n"
			+ "local result = redis.call('lrange', KEYS[1], 0, batchsize - 1)\n"
			+ "redis.call('ltrim', KEYS[1], batchsize, -1)\n"
			+ "return result";
	
	public void execScript(){
		while(true){
			List resultList = redisTemplate.execute(script(), Collections.singletonList("VALUE"), batchSize);
			logger.info("resultList: {}", resultList);
			
			if(resultList.size() == 0){
				try {
					Thread.sleep(waitTimeInMS);
				} catch (InterruptedException e) {
					logger.error(e.getMessage(), e);
				}
			}
		}
	}
	
	/*
	private String scriptSha;
	public void loadScript(){
		Object result = template.execute(new RedisCallback<Object>(){

			@Override
			public Object doInRedis(RedisConnection connection) throws DataAccessException {
				return connection.scriptLoad(luaScript.getBytes());
			}
		});
		
		logger.info("Sha1: {}", result);
		scriptSha = (String) result;
	}

	public void exec() {
		List<Object> results = template.executePipelined(new RedisCallback<Object>() {
			public Object doInRedis(RedisConnection connection) throws DataAccessException {
				StringRedisConnection stringRedisConn = (StringRedisConnection) connection;
				stringRedisConn.evalSha(scriptSha, ReturnType.MULTI, 1, "VALUE");
				return null;
			}
		});
		
		logger.info("results: {}", results);
	}
	*/

}