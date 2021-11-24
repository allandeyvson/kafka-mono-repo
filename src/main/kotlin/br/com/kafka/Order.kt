package br.com.kafka

import java.math.BigDecimal

data class Order(val userId: String, val orderId: String, val amount: BigDecimal)
