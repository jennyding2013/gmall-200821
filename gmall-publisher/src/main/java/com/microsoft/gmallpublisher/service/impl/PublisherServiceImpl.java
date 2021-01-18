package com.microsoft.gmallpublisher.service.impl;

import com.microsoft.gmallpublisher.mapper.DauMapper;
import com.microsoft.gmallpublisher.mapper.OrderMapper;
import com.microsoft.gmallpublisher.service.PublisherService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author Jenny.D
 * @create 2021-01-09 14:04
 */

@Service
public class PublisherServiceImpl implements PublisherService {

    @Autowired
    private DauMapper dauMapper;

    @Autowired
    private OrderMapper orderMapper;

    @Override
    public Integer getDauTotal(String date) {
        return dauMapper.selectDauTotal(date);
    }

    @Override
    public Map getDauTotalHourMap(String date) {
        List<Map> list = dauMapper.selectDauTotalHourMap(date);

        HashMap<String, Long> result = new HashMap<>();

        for(Map map:list){
            result.put((String) map.get("LH"),(Long)map.get("CT"));
        }

        return result;
    }

    @Override
    public Double getOrderAmount(String date) {
        return orderMapper.selectOrderAmountTotal(date);
    }

    @Override
    public Map getOrderAmountHour(String date) {
        List<Map> list = orderMapper.selectOrderAmountHourMap(date);

        HashMap<String, Double> result = new HashMap<>();

        for(Map map:list){
            result.put((String)map.get("CREATE_HOUR"),(Double)map.get("SUM_AMOUNT"));
}
        return result;
    }
}
