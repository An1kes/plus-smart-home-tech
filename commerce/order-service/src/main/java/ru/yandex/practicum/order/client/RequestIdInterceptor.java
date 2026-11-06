package ru.yandex.practicum.order.client;

import feign.RequestInterceptor;
import feign.RequestTemplate;
import jakarta.servlet.http.HttpServletRequest;
import org.springframework.stereotype.Component;
import org.springframework.web.context.request.RequestContextHolder;
import org.springframework.web.context.request.ServletRequestAttributes;

import java.util.UUID;

@Component
public class RequestIdInterceptor implements RequestInterceptor {

    private static final String REQUEST_ID_HEADER = "X-Request-Id";

    @Override
    public void apply(RequestTemplate template) {
        String requestId = null;

        ServletRequestAttributes attributes = (ServletRequestAttributes) RequestContextHolder.getRequestAttributes();
        if (attributes != null) {
            HttpServletRequest request = attributes.getRequest();
            requestId = request.getHeader(REQUEST_ID_HEADER);
        }

        if (requestId == null || requestId.isBlank()) {
            requestId = UUID.randomUUID().toString();
        }

        template.header(REQUEST_ID_HEADER, requestId);
    }
}