package com.vietmart.dashboard.client;

import org.springframework.cloud.openfeign.FeignClient;
import org.springframework.web.bind.annotation.GetMapping;

@FeignClient(name = "user-service")
public interface UserClient {

    @GetMapping("/internal/users/new-this-month")
    Long getNewUsersThisMonth();
}
