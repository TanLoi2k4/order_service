package com.tlcn.order_service.util;

import java.math.BigDecimal;
import java.math.RoundingMode;

public final class MoneyUtils {

    private MoneyUtils() {}

    public static BigDecimal safeMoney(Double value) {
        if (value == null) {
            return BigDecimal.ZERO.setScale(2, RoundingMode.HALF_UP);
        }
        BigDecimal bd = BigDecimal.valueOf(value);
        if (bd.compareTo(BigDecimal.ZERO) < 0) {
            return BigDecimal.ZERO.setScale(2, RoundingMode.HALF_UP);
        }
        return bd.setScale(2, RoundingMode.HALF_UP);
    }

    public static BigDecimal normalizeMoney(BigDecimal value) {
        if (value == null) {
            return BigDecimal.ZERO.setScale(2, RoundingMode.HALF_UP);
        }
        BigDecimal normalized = value;
        if (normalized.compareTo(BigDecimal.ZERO) < 0) {
            normalized = BigDecimal.ZERO;
        }
        return normalized.setScale(2, RoundingMode.HALF_UP);
    }
}
