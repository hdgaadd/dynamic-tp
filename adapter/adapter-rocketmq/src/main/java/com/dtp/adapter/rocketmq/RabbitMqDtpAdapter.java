package com.dtp.adapter.rocketmq;

import com.dtp.adapter.common.AbstractDtpAdapter;
import com.dtp.common.dto.ExecutorWrapper;
import com.dtp.common.properties.DtpProperties;
import com.dtp.common.util.ReflectionUtil;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.springframework.amqp.rabbit.connection.AbstractConnectionFactory;

import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadPoolExecutor;

/**
 * RabbitMqDtpAdapter related
 *
 * @author hdgaadd
 * @since 1.0.6
 */
@SuppressWarnings("all")
@Slf4j
@RequiredArgsConstructor
public class RabbitMqDtpAdapter extends AbstractDtpAdapter {

    private static final String NAME = "rocketMqTp";

    private static final String CONSUME_EXECUTOR_FIELD_NAME = "executorService";

    private final Map<String, AbstractConnectionFactory> abstractConnectionFactoryMap;

    @Override
    public void refresh(DtpProperties dtpProperties) {
        refresh(NAME, dtpProperties.getRocketMqTp(), dtpProperties.getPlatforms());
    }

    @Override
    protected void initialize() {
        super.initialize();

        abstractConnectionFactoryMap.forEach((beanName, abstractConnectionFactor) -> {
            ExecutorService executor = (ExecutorService) ReflectionUtil.getFieldValue(AbstractConnectionFactory.class, CONSUME_EXECUTOR_FIELD_NAME, abstractConnectionFactor);
            if (Objects.nonNull(executor)) {
                if (executor instanceof ThreadPoolExecutor) {
                    ThreadPoolExecutor threadPoolTaskExecutor = (ThreadPoolExecutor) executor;

                    String key = beanName;
                    val executorWrapper = new ExecutorWrapper(key, executor);
                    initNotifyItems(key, executorWrapper);
                    executors.put(key, executorWrapper);

                    log.info("DynamicTp adapter, rabbitMq consumer executors init end, executors: {}", executors);
                } else {
                    log.warn("Cannot find beans of type AbstractConnectionFactory.");
                    return;
                }
            }
        });
    }
}
