package com.accor.tech.factory;

import com.accor.tech.api.internal.StreamExecutor;
import com.accor.tech.config.ListenerConfig;
import com.accor.tech.processor.ActionProcessorAccor;
import lombok.AllArgsConstructor;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Scope;

@AllArgsConstructor
@Configuration
public class FactorySocleEventDriven {

    private final StreamExecutor streamExecutor;
    private final ListenerConfig listenerConfig;

    @Bean
    @Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
    public ActionProcessorAccor actionProcessorAccor() {
        return new ActionProcessorAccor(streamExecutor, listenerConfig);
    }
}
