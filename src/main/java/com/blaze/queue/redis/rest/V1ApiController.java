package com.blaze.queue.redis.rest;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import javax.annotation.PostConstruct;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.util.Assert;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestController;

import com.blaze.queue.QueueService;
import com.blaze.queue.data.TextData;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;

@RestController
@RequestMapping("/rmq/api")
public class V1ApiController {

	public static final String BADREQ_INV_JSON = "Not a valid JSON";
	public static final String BADREQ_INV_JSONARR = "Expecting a JSON array";
	private static final Logger log = LoggerFactory.getLogger(V1ApiController.class);
	
	@Autowired
	QueueService service;
	private ObjectMapper om;
	@PostConstruct
	private void init()
	{
		om = new ObjectMapper();
	}
	/**
	 * 
	 * @param queue
	 * @param json
	 * @return
	 * @throws IOException 
	 * @throws JsonProcessingException 
	 */
	@RequestMapping(method = {RequestMethod.POST}, path = "/add/{queue}")
	public int addToQueue(@PathVariable("queue") String queue, @RequestBody String json) throws JsonProcessingException, IOException
	{
		om.reader().readTree(json);
		log.info("Adding to queue - ["+queue+"] "+json);
		return service.add(Arrays.asList(new TextData(json)), queue);
	}
	/**
	 * Add an array of json objects to queue
	 * @param queue
	 * @param jsonArray
	 * @throws IOException 
	 * @throws JsonProcessingException 
	 */
	@RequestMapping(method = {RequestMethod.POST}, path = "/ingest/{queue}")
	public void ingestToQueue(@PathVariable("queue") String queue, @RequestBody String jsonArray) throws JsonProcessingException, IOException
	{
		JsonNode root = om.reader().readTree(jsonArray);
		Assert.isTrue(root.isArray(), "Not a JSON array");
		JsonNode each;
		ObjectWriter ow = om.writer();
		List<TextData> list = new ArrayList<>();
		for(Iterator<JsonNode> iter = root.elements();iter.hasNext();)
		{
			each = iter.next();
			list.add(new TextData(ow.writeValueAsString(each), queue));
		}
		log.info("Adding to queue - ["+queue+"] "+list);
		service.ingest(list, queue);
	}
	
	@ResponseStatus(value=HttpStatus.BAD_REQUEST, reason=BADREQ_INV_JSON)
	@ExceptionHandler({JsonProcessingException.class, IOException.class})
	public void onMalformedJson(Throwable e){
		log.warn(BADREQ_INV_JSON, e);
	}
	@ResponseStatus(value=HttpStatus.BAD_REQUEST, reason=BADREQ_INV_JSONARR)
	@ExceptionHandler({IllegalArgumentException.class})
	public void onMalformedJsonArray(Throwable e){
		log.warn(BADREQ_INV_JSONARR, e);
	}
}
