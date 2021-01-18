package com.microsoft.utils;

import java.util.Random;

/**
 * @author Jenny.D
 * @create 2021-01-05 18:17
 */
public class RandomNum {
    public static int getRandInt(int fromNum, int toNum) {
        return fromNum + new Random().nextInt(toNum - fromNum + 1);
    }
}
