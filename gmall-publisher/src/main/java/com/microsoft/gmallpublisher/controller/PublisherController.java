package com.microsoft.gmallpublisher.controller;

import com.alibaba.fastjson.JSONObject;
import com.microsoft.gmallpublisher.service.PublisherService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.time.LocalDate;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

/**
 * @author Jenny.D
 * @create 2021-01-09 13:01
 */
@RestController
public class PublisherController {
    @Autowired
    private PublisherService publisherService;

    @RequestMapping("realtime-total")
    public String getDauTotal(@RequestParam("date") String date){
        ArrayList<Map> result = new ArrayList<>();

        Integer dauTotal = publisherService.getDauTotal(date);
        Double orderAmount = publisherService.getOrderAmount(date);

        HashMap<String, Object> dauMap = new HashMap<>();
        dauMap.put("id","dau");

        dauMap.put("name","新增日活");

        dauMap.put("value",dauTotal);

        HashMap<String, Object> newMidMap = new HashMap<>();
        newMidMap.put("id","new_mid");
        newMidMap.put("name","新增设备");
        newMidMap.put("value",233);

        HashMap<String, Object> gmvMap = new HashMap<>();
        gmvMap.put("id","order_amount");
        gmvMap.put("name","新增交易额");
        gmvMap.put("value",orderAmount);

        result.add(dauMap);
        result.add(newMidMap);
        result.add(gmvMap);

        return JSONObject.toJSONString(result);
    }

    @RequestMapping("realtime-hours")
    public String getDauTotalHourMap(@RequestParam("id") String id,
                                     @RequestParam("date") String date){
        HashMap<String, Map> result = new HashMap<>();

        String yesterday = LocalDate.parse(date).plusDays(-1).toString();

        Map todayMap = null;
        Map yesterdayMap = null;

        if("dau".equals(id)){
            todayMap = publisherService.getDauTotalHourMap(date);
            yesterdayMap = publisherService.getDauTotalHourMap(yesterday);
        }else if("order_amount".equals(id)){
            todayMap = publisherService.getOrderAmountHour(date);
            yesterdayMap = publisherService.getOrderAmountHour(yesterday);
        }

        result.put("yesterday",yesterdayMap);

        result.put("today",todayMap);

        return JSONObject.toJSONString(result);

    }


}
