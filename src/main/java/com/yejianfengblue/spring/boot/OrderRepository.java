package com.yejianfengblue.spring.boot;

import org.springframework.data.mongodb.repository.MongoRepository;
import org.springframework.stereotype.Repository;

@Repository("oldOrderRepos")
interface OrderRepository extends MongoRepository<Order, String> {}