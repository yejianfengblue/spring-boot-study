package com.yejianfengblue.spring.boot;

import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.ToString;

@Getter
@RequiredArgsConstructor
@ToString
class OrderPaidEvent {

    private final String orderId;
}
