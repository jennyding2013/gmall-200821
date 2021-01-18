package com.microsoft.bean

/**
 * @author Jenny.D
 * @create 2021-01-12 20:53
 */
case class CouponAlertInfo(mid:String,
                           uids:java.util.HashSet[String],
                           itemIds:java.util.HashSet[String],
                           events:java.util.List[String],
                           ts:Long)
