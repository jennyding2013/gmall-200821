package com.microsoft.gmalllogger.controller;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.microsoft.constants.GmallConstant;
import lombok.extern.slf4j.Slf4j;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

//@RestController = @Controller+@ResponseBody
//@Controller
@RestController

@Slf4j
public class LoggerController {

    @Autowired
    private KafkaTemplate<String,String> kafkaTemplate;

    @RequestMapping("test1")
    //@ResponseBody

    public String test01(){
        System.out.println("aaaaaa");

        return "success";
    }

    @RequestMapping("test2")

    public String test02(@RequestParam("name") String nn,@RequestParam("age") int age){
        System.out.println(nn+":"+age );
        return "success";
    }

    @RequestMapping("log")
    public String getLog(@RequestParam("logString") String logStr){
      //  System.out.println(logStr);

        JSONObject jsonObject = JSON.parseObject(logStr);
        jsonObject.put("ts",System.currentTimeMillis());

        String logger = jsonObject.toString();
        log.info(logger);

        if("startup".equals(jsonObject.getString("type"))){
            kafkaTemplate.send(GmallConstant.GMALL_STARTUP,logger);
        }else{
            kafkaTemplate.send(GmallConstant.GMALL_EVENT,logger);
        }

        return "success";

    }

}
