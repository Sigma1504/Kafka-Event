package com.accor.tech.adminevent.service;

import com.accor.tech.domain.EventActionStream;
import com.accor.tech.listener.ListenerActionService;
import com.accor.tech.listener.ProducerEventService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.util.Date;

@Slf4j
@Component
public class ActionStreamService {

    private final ProducerEventService producerEventService;

    public ActionStreamService(ProducerEventService producerActionService) {
        this.producerEventService = producerActionService;
    }

    public void launchAction(EventActionStream eventActionStream) {
        producerEventService.produceEventActionStream(ListenerActionService.TOPIC_ACTION_ACCOR, eventActionStream.withDateGeneration(new Date()));
    }

}
