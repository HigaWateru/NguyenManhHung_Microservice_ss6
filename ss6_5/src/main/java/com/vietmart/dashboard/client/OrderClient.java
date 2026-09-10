package com.vietmart.dashboard.client;

import org.springframework.cloud.openfeign.FeignClient;
import org.springframework.web.bind.annotation.GetMapping;

@FeignClient(name = "order-service")
public interface OrderClient {

    @GetMapping("/internal/orders/count")
    Long getTotalCount();

    @GetMapping("/internal/orders/revenue/week")
    Long getWeekRevenue();
}
