# Bài tập 5: API Aggregator Pattern cho Dashboard VietMart

## 1. Phân tích vấn đề hiệu năng

Dashboard cần lấy bốn dữ liệu độc lập:

- Tổng số đơn hàng.
- Doanh thu trong tuần.
- Số sản phẩm đang bán.
- Số user mới trong tháng.

Với cách gọi tuần tự, lời gọi sau chỉ bắt đầu khi lời gọi trước đã hoàn thành:

```text
Tổng thời gian = T_orders + T_revenue + T_products + T_users
               = 200ms + 200ms + 200ms + 200ms
               = khoảng 800ms
```

Bốn lời gọi không phụ thuộc dữ liệu của nhau, vì vậy có thể khởi chạy cùng lúc. Khi chạy song song, tổng thời gian xấp xỉ lời gọi chậm nhất:

```text
Tổng thời gian = max(T_orders, T_revenue, T_products, T_users)
               = max(200ms, 200ms, 200ms, 200ms)
               = khoảng 200ms
```

Thực tế có thể lớn hơn một chút do thời gian tạo task, lập lịch thread, kết nối mạng và tổng hợp kết quả, nhưng vẫn nằm gần mục tiêu 250ms nếu các service đáp ứng đủ nhanh.

> Lưu ý: `CompletableFuture` chỉ tạo ra tính song song ở phía Aggregator. Nếu Feign vẫn dùng client blocking, cần chạy các lời gọi blocking trên một thread pool riêng, không nên chiếm các thread xử lý request của web server.

## 2. Feign clients

Ví dụ các client gọi những service nội bộ của VietMart:

```java
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
```

```java
package com.vietmart.dashboard.client;

import org.springframework.cloud.openfeign.FeignClient;
import org.springframework.web.bind.annotation.GetMapping;

@FeignClient(name = "product-service")
public interface ProductClient {

    @GetMapping("/internal/products/active/count")
    Long getActiveCount();
}
```

```java
package com.vietmart.dashboard.client;

import org.springframework.cloud.openfeign.FeignClient;
import org.springframework.web.bind.annotation.GetMapping;

@FeignClient(name = "user-service")
public interface UserClient {

    @GetMapping("/internal/users/new-this-month")
    Long getNewUsersThisMonth();
}
```

## 3. DTO và cấu hình thread pool

```java
package com.vietmart.dashboard.dto;

public record DashboardResponse(
        Long totalOrders,
        Long weekRevenue,
        Long activeProducts,
        Long newUsersThisMonth
) {
}
```

```java
package com.vietmart.dashboard.config;

import java.util.concurrent.Executor;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

@Configuration
public class DashboardAsyncConfig {

    @Bean("dashboardExecutor")
    public Executor dashboardExecutor() {
        ThreadPoolTaskExecutor executor = new ThreadPoolTaskExecutor();
        executor.setCorePoolSize(4);
        executor.setMaxPoolSize(8);
        executor.setQueueCapacity(32);
        executor.setThreadNamePrefix("dashboard-");
        executor.initialize();
        return executor;
    }
}
```

Pool có tối thiểu bốn thread để bốn lời gọi có thể thực sự chạy đồng thời trong trường hợp một request dashboard. Kích thước thực tế cần được điều chỉnh theo số request đồng thời, độ trễ downstream và giới hạn connection pool của Feign/HTTP client.

## 4. Dashboard Aggregator dùng CompletableFuture

```java
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
        CompletableFuture<Long> totalOrders = CompletableFuture
                .supplyAsync(orderClient::getTotalCount, dashboardExecutor)
                .exceptionally(exception -> 0L);

        CompletableFuture<Long> weekRevenue = CompletableFuture
                .supplyAsync(orderClient::getWeekRevenue, dashboardExecutor)
                .exceptionally(exception -> 0L);

        CompletableFuture<Long> activeProducts = CompletableFuture
                .supplyAsync(productClient::getActiveCount, dashboardExecutor)
                .exceptionally(exception -> 0L);

        CompletableFuture<Long> newUsersThisMonth = CompletableFuture
                .supplyAsync(userClient::getNewUsersThisMonth, dashboardExecutor)
                .exceptionally(exception -> -1L);

        CompletableFuture<Void> allCalls = CompletableFuture.allOf(
                totalOrders,
                weekRevenue,
                activeProducts,
                newUsersThisMonth
        );

        try {
            allCalls.get(3, TimeUnit.SECONDS);

            return new DashboardResponse(
                    totalOrders.join(),
                    weekRevenue.join(),
                    activeProducts.join(),
                    newUsersThisMonth.join()
            );
        } catch (InterruptedException exception) {
            Thread.currentThread().interrupt();
            return fallbackDashboard();
        } catch (java.util.concurrent.TimeoutException exception) {
            // Timeout tổng: không để request chờ vô hạn.
            return fallbackDashboard();
        } catch (java.util.concurrent.ExecutionException exception) {
            return fallbackDashboard();
        }
    }

    private DashboardResponse fallbackDashboard() {
        return new DashboardResponse(0L, 0L, 0L, -1L);
    }
}
```

Controller chỉ điều phối request đến Aggregator:

```java
package com.vietmart.dashboard.controller;

import com.vietmart.dashboard.dto.DashboardResponse;
import com.vietmart.dashboard.service.DashboardService;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/admin")
public class DashboardController {

    private final DashboardService dashboardService;

    public DashboardController(DashboardService dashboardService) {
        this.dashboardService = dashboardService;
    }

    @GetMapping("/dashboard")
    public DashboardResponse getDashboard() {
        return dashboardService.getDashboard();
    }
}
```

### Vì sao dùng `exceptionally()` trên từng future?

Mỗi future có fallback riêng. Nếu `product-service` lỗi, `activeProducts` nhận `0L`, trong khi ba future còn lại vẫn giữ được kết quả thật. Do đó aggregator vẫn trả được một dashboard một phần thay vì biến một lỗi cục bộ thành lỗi toàn bộ màn hình.

Quy ước giá trị mặc định trong ví dụ:

- `0`: số lượng hoặc doanh thu không lấy được được hiển thị tạm như không có dữ liệu.
- `-1`: dùng để biểu thị dữ liệu user chưa xác định, giúp frontend phân biệt với giá trị thật bằng 0.

Trong hệ thống thực tế, nên trả thêm trạng thái như `degraded`, `partialData` hoặc trường `dataStatus` thay vì chỉ dựa vào số âm. Frontend cũng nên hiển thị cảnh báo dữ liệu tạm thời không đầy đủ.

### Ý nghĩa của timeout 3 giây

```java
allCalls.get(3, TimeUnit.SECONDS);
```

Dòng này giới hạn thời gian chờ của toàn bộ nhóm lời gọi. Nếu một task bị treo quá lâu, aggregator không chờ vô hạn mà trả response fallback. Tuy nhiên, timeout ở aggregator không nhất thiết hủy được HTTP request đang chạy bên dưới. Cần cấu hình thêm connect timeout/read timeout của Feign và cơ chế circuit breaker để giải phóng tài nguyên downstream.

Nếu yêu cầu nghiêm ngặt là deadline 250ms, timeout 3 giây chỉ là giới hạn bảo vệ ở mức cuối, không phải SLA thực tế. Nên đặt timeout Feign nhỏ hơn deadline của endpoint, dành một phần ngân sách cho xử lý và serialize response.

## 5. So sánh hai cách tiếp cận

| Tiêu chí | Gọi tuần tự | `CompletableFuture` song song |
|---|---|---|
| Độ trễ | Gần bằng tổng độ trễ các service | Gần bằng độ trễ service chậm nhất |
| Độ phức tạp | Dễ đọc, dễ viết và dễ debug | Nhiều future, timeout, fallback và executor hơn |
| Xử lý lỗi | Luồng thường dừng ngay khi một lời gọi lỗi | Có thể cô lập lỗi từng service |
| Tài nguyên | Ít thread/tác vụ đồng thời hơn | Dùng nhiều thread, socket và connection hơn |
| Khả năng mở rộng | Không tốt khi dashboard có nhiều nguồn dữ liệu | Tốt hơn cho các lời gọi độc lập, nhưng cần giới hạn concurrency |
| Debugging/Tracing | Trace tuyến tính, đơn giản | Cần correlation ID, tracing và log theo từng future |

### Ưu điểm của gọi song song

1. Giảm đáng kể latency tổng khi các lời gọi độc lập.
2. Một service lỗi có thể được thay bằng fallback mà không làm dashboard biến mất.
3. Dễ mở rộng aggregator để kết hợp thêm các nguồn dữ liệu độc lập, miễn là kiểm soát được tài nguyên.

### Nhược điểm và rủi ro

1. Code khó đọc và khó debug hơn vì kết quả hoàn thành không nhất thiết theo thứ tự tạo task.
2. Feign blocking chạy trên thread pool; nếu cấu hình pool quá nhỏ, các task vẫn bị xếp hàng và mất lợi ích song song.
3. Nếu cấu hình pool quá lớn, hệ thống có thể tạo nhiều request đồng thời, gây cạn thread, connection pool hoặc làm quá tải service downstream.
4. Fallback `0` có thể khiến người dùng hiểu nhầm là dữ liệu thật bằng 0. Cần kèm trạng thái dữ liệu và log lỗi.
5. Khi timeout xảy ra, request downstream có thể vẫn tiếp tục nếu client không hỗ trợ hủy, làm tăng tải không cần thiết.
6. Nên bổ sung circuit breaker, bulkhead, retry có giới hạn và metrics cho từng dependency. Retry phải thận trọng để không khuếch đại sự cố dây chuyền.

## 6. Kết luận

API Aggregator là lựa chọn phù hợp cho Dashboard VietMart vì bốn dữ liệu không phụ thuộc lẫn nhau. Với `CompletableFuture.supplyAsync()` và executor riêng, bốn lời gọi được khởi chạy song song; thời gian phản hồi lý tưởng giảm từ khoảng 800ms xuống khoảng 200ms, gần với mục tiêu 250ms. `CompletableFuture.allOf(...).get(3, TimeUnit.SECONDS)` đảm bảo có timeout tổng, còn `exceptionally()` giúp dashboard vẫn hiển thị khi một dependency gặp sự cố.

Để đưa vào production, cần đo latency thực tế, cấu hình timeout HTTP nhỏ hơn SLA, giới hạn thread/connection pool, thêm tracing và thể hiện rõ cho frontend khi response chỉ chứa dữ liệu một phần.

## 7. Mã nguồn trong repository

Project đã có mã nguồn Spring Boot tương ứng với thiết kế trên:

```text
src/main/java/com/vietmart/dashboard/
├── DashboardApplication.java
├── client/
│   ├── OrderClient.java
│   ├── ProductClient.java
│   └── UserClient.java
├── config/DashboardAsyncConfig.java
├── controller/DashboardController.java
├── dto/DashboardResponse.java
└── service/DashboardService.java
```

Chạy project bằng Maven:

```bash
mvn spring-boot:run
```

Endpoint aggregator là `GET /api/admin/dashboard`. Các Feign client đang gọi lần lượt
`order-service`, `product-service` và `user-service`, vì vậy cần service discovery/configuration
hoặc mock server tương ứng trước khi gọi endpoint. `application.yml` đặt HTTP timeout của Feign
ở mức 100ms connect và 200ms read để phù hợp với mục tiêu latency minh họa.

Kết quả 800ms và 200ms trong tài liệu là phép tính lý thuyết với bốn dependency có độ trễ 200ms.
Muốn ghi nhận số đo thực tế cần chạy các downstream service/mock có độ trễ kiểm soát, gọi endpoint
nhiều lần và báo cáo cả average, p95/p99 cùng trạng thái lỗi hoặc timeout.
