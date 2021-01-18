package com.microsoft.gmallpublisher.mapper;

import java.util.List;
import java.util.Map;

/**
 * @author Jenny.D
 * @create 2021-01-09 13:04
 */
public interface DauMapper {
    public Integer selectDauTotal(String date);

    public List<Map> selectDauTotalHourMap(String date);

}
