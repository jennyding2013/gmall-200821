package com.microsoft.gmallpublisher.mapper;

import java.util.List;
import java.util.Map;

/**
 * @author Jenny.D
 * @create 2021-01-11 18:26
 */
public interface OrderMapper {
    public Double selectOrderAmountTotal(String date);

    public List<Map> selectOrderAmountHourMap(String date);

}
