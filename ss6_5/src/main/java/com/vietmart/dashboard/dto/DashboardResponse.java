package com.vietmart.dashboard.dto;

public record DashboardResponse(
        Long totalOrders,
        Long weekRevenue,
        Long activeProducts,
        Long newUsersThisMonth
) {
}
