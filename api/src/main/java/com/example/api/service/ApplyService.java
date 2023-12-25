package com.example.api.service;

import com.example.api.domain.Coupon;
import com.example.api.producer.CouponCreateProducer;
import com.example.api.repository.AppliedUserRepository;
import com.example.api.repository.CouponCountRepository;
import com.example.api.repository.CouponRepository;
import org.springframework.stereotype.Service;

@Service
public class ApplyService {

    private final CouponRepository couponRepository;
    private final CouponCountRepository couponCountRepository;
    private final CouponCreateProducer couponCreateProducer;
    private final AppliedUserRepository appliedUserRepository;

    public ApplyService(CouponRepository couponRepository, CouponCountRepository couponCountRepository, CouponCreateProducer couponCreateProducer, AppliedUserRepository appliedUserRepository) {
        this.couponRepository = couponRepository;
        this.couponCountRepository = couponCountRepository;
        this.couponCreateProducer = couponCreateProducer;
        this.appliedUserRepository = appliedUserRepository;
    }

    public void apply(Long userId) { // synchronized를 사용하여 처리하면 다중 서버에서는 제대로 동작하지 않음
        // mysql, redis lock을 사용하면 해결할 수 있지만 lock을 거는 구간이 길어지면 성능에 불이익이 있을 수 있음
        // 여기서 필요한건 쿠폰개수에 대한 정합성만 따지면 됨 -> redis 활용
        long count = couponRepository.count();

        if (count > 100) {
            return;
        }

        couponRepository.save(new Coupon(userId));
    }

    public void applyWithRedis(Long userId) {
        long count = couponCountRepository.increment();

        if (count > 100) {
            return;
        }

        couponRepository.save(new Coupon(userId));
        // 발급하는 쿠폰의 개수가 많아지면 많아질수록 rdb에 부하를 주게됨 -> 카프카 활용
    }

    public void applyWithRedisAndKafka(Long userId) {
        // lock start
        // 쿠폰 발급 여부
        // if(발급됐다면) return
        long count = couponCountRepository.increment();

        if (count > 100) {
            return;
        }

        couponCreateProducer.create(userId);
        // lock end
        // lock을 사용하여 쿠폰 발급여부를 확인하는 방식 -> 카프카 이벤트를 통해 쿠폰이 생성되므로 서비스에서 락을 걸게되면 쿠폰이 여러개 발급될 수 있음
    }

    public void applyWithRedisAndKafka2(Long userId) {

        if (appliedUserRepository.add(userId) != 1) {
            return;
        }

        long count = couponCountRepository.increment();

        if (count > 100) {
            return;
        }

        couponCreateProducer.create(userId);
    }
}
