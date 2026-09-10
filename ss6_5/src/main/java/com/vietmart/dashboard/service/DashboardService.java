package com.vietmart.dashboard.service;

import com.vietmart.dashboard.client.OrderClient;
import com.vietmart.dashboard.client.ProductClient;
import com.vietmart.dashboard.client.UserClient;
import com.vietmart.dashboard.dto.DashboardResponse;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;

@Service
public class DashboardService {

    private final OrderClient orderClient;
    private final ProductClient productClient;
    private final UserClient userClient;
    private final Executor dashboardExecutor;

    public DashboardService(
            OrderClient orderClient,
            ProductClient productClient,
            UserClient userClient,
            @Qualifier("dashboardExecutor") Executor dashboardExecutor) {
        this.orderClient = orderClient;
        this.productClient = productClient;
        this.userClient = userClient;
        this.dashboardExecutor = dashboardExecutor;
    }

    public DashboardResponse getDashboard() {
        CompletableFuture<Long> totalOrders = call(orderClient::getTotalCount, 0L);
        CompletableFuture<Long> weekRevenue = call(orderClient::getWeekRevenue, 0L);
        CompletableFuture<Long> activeProducts = call(productClient::getActiveCount, 0L);
        CompletableFuture<Long> newUsersThisMonth = call(userClient::getNewUsersThisMonth, -1L);

        CompletableFuture<Void> allCalls = CompletableFuture.allOf(
                totalOrders,
                weekRevenue,
                activeProducts,
                newUsersThisMonth);

        try {
            allCalls.get(3, TimeUnit.SECONDS);
            return new DashboardResponse(
                    totalOrders.join(),
                    weekRevenue.join(),
                    activeProducts.join(),
                    newUsersThisMonth.join());
        } catch (InterruptedException exception) {
            Thread.currentThread().interrupt();
            return fallbackDashboard();
        } catch (java.util.concurrent.TimeoutException | java.util.concurrent.ExecutionException exception) {
            return fallbackDashboard();
        }
    }

    private CompletableFuture<Long> call(java.util.function.Supplier<Long> clientCall, Long fallback) {
        return CompletableFuture
                .supplyAsync(clientCall, dashboardExecutor)
                .exceptionally(exception -> fallback);
    }

    private DashboardResponse fallbackDashboard() {
        return new DashboardResponse(0L, 0L, 0L, -1L);
    }
}
