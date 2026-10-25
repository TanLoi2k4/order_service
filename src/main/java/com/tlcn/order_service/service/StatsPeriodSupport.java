package com.tlcn.order_service.service;

import com.tlcn.order_service.enums.StatsPeriod;

import java.time.DayOfWeek;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.time.temporal.TemporalAdjusters;
import java.time.temporal.WeekFields;

/**
 * Resolves week/month/year ranges in Vietnam time for vendor order statistics.
 * Range bounds are half-open: {@code from} inclusive, {@code to} exclusive.
 */
public final class StatsPeriodSupport {

    public static final ZoneId ZONE = ZoneId.of("Asia/Ho_Chi_Minh");

    private static final DateTimeFormatter DAY_MONTH = DateTimeFormatter.ofPattern("dd/MM");
    private static final DateTimeFormatter DAY_MONTH_YEAR = DateTimeFormatter.ofPattern("dd/MM/yyyy");
    private static final WeekFields ISO_WEEK = WeekFields.ISO;

    private StatsPeriodSupport() {
    }

    public record Range(Instant from, Instant to, String label) {
    }

    public static LocalDate today() {
        return LocalDate.now(ZONE);
    }

    public static LocalDate normalizeAnchor(LocalDate anchor) {
        return anchor != null ? anchor : today();
    }

    public static Range resolve(StatsPeriod period, LocalDate anchor) {
        StatsPeriod safePeriod = period == null ? StatsPeriod.ALL : period;
        LocalDate date = normalizeAnchor(anchor);
        return switch (safePeriod) {
            case ALL -> new Range(null, null, "Tất cả thời gian");
            case WEEK -> weekRange(date);
            case MONTH -> monthRange(date);
            case YEAR -> yearRange(date);
        };
    }

    private static Range weekRange(LocalDate date) {
        LocalDate start = date.with(TemporalAdjusters.previousOrSame(DayOfWeek.MONDAY));
        LocalDate endExclusive = start.plusWeeks(1);
        LocalDate endInclusive = start.plusDays(6);
        int week = start.get(ISO_WEEK.weekOfWeekBasedYear());
        int weekYear = start.get(ISO_WEEK.weekBasedYear());
        String label = "Tuần " + week + "/" + weekYear
            + " (" + start.format(DAY_MONTH) + " – " + endInclusive.format(DAY_MONTH_YEAR) + ")";
        return toRange(start, endExclusive, label);
    }

    private static Range monthRange(LocalDate date) {
        LocalDate start = date.withDayOfMonth(1);
        LocalDate endExclusive = start.plusMonths(1);
        String label = "Tháng " + start.getMonthValue() + "/" + start.getYear();
        return toRange(start, endExclusive, label);
    }

    private static Range yearRange(LocalDate date) {
        LocalDate start = date.withDayOfYear(1);
        LocalDate endExclusive = start.plusYears(1);
        String label = "Năm " + start.getYear();
        return toRange(start, endExclusive, label);
    }

    private static Range toRange(LocalDate start, LocalDate endExclusive, String label) {
        return new Range(
            start.atStartOfDay(ZONE).toInstant(),
            endExclusive.atStartOfDay(ZONE).toInstant(),
            label
        );
    }
}
