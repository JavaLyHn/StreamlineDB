package com.lyhn.streamlinedb.backend.utils;

import java.security.SecureRandom;
import java.util.Random;

public class RandomUtil {
    private static final SecureRandom random = new SecureRandom();

    public static byte[] randomBytes(int length) {
        byte[] bytes = new byte[length];
        random.nextBytes(bytes);
        return bytes;
    }
}
