package com.microsoft.gmallpublisher.service;

import java.util.Map;

/**
 * @author Jenny.D
 * @create 2021-01-09 13:03
 */

public interface PublisherService {
    public Integer getDauTotal(String date);

    public Map getDauTotalHourMap(String date);

    public Double getOrderAmount(String date);

    public Map getOrderAmountHour(String date);
}
