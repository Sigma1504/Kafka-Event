package com.accor.tech.adminevent.web;

import com.accor.tech.adminevent.repository.StatusRepository;
import com.accor.tech.adminevent.service.ActionStreamService;
import com.accor.tech.adminevent.service.KafkaSimulateReadService;
import com.accor.tech.adminevent.web.domain.PayloadReadOutput;
import com.accor.tech.domain.EventActionStream;
import com.accor.tech.domain.TypeStatus;
import lombok.extern.slf4j.Slf4j;
import org.springframework.web.bind.annotation.*;

import java.util.List;

import static org.springframework.http.HttpStatus.CREATED;
import static org.springframework.http.HttpStatus.OK;

@RestController
@RequestMapping("/manage")
@Slf4j
public class ManageController {

    private final ActionStreamService actionStreamService;
    private final StatusRepository statusRepository;
    private final KafkaSimulateReadService kafkaSimulateReadService;

    public ManageController(ActionStreamService actionStreamService, StatusRepository statusRepository, KafkaSimulateReadService kafkaSimulateReadService) {
        this.actionStreamService = actionStreamService;
        this.statusRepository = statusRepository;
        this.kafkaSimulateReadService = kafkaSimulateReadService;
    }

    @ResponseStatus(CREATED)
    @PostMapping("/action")
    public void action(@RequestBody EventActionStream eventActionStream) {
        actionStreamService.launchAction(eventActionStream);
    }

    @GetMapping("/findAll")
    public List<EventActionStream> findAll() {
        return statusRepository.findAll();
    }

    @GetMapping("/findByStatus")
    public List<EventActionStream> findByStatus(@RequestParam("status") TypeStatus status) {
        return statusRepository.findByStatus(status);
    }

    @GetMapping("/findByInstanceName")
    public List<EventActionStream> findByInstanceName(@RequestParam("instanceName") String instanceName) {
        return statusRepository.findByInstanceName(instanceName);
    }


    @GetMapping("/findByStreamName")
    public EventActionStream findByStreamName(@RequestParam("streamName") String streamName) {
        return statusRepository.findByKey(streamName);
    }

    @ResponseStatus(OK)
    @PostMapping("/raw/captureRaw")
    public List<String> captureRaw(@RequestBody PayloadReadOutput payloadReadOutput) {
        return kafkaSimulateReadService.readKafkaRawData(payloadReadOutput.getBootStrapServers(), payloadReadOutput.getTopic(), payloadReadOutput.getMaxRecords(), payloadReadOutput.getWindowTime(), payloadReadOutput.getOffset(), payloadReadOutput.getDeserializer());
    }

}

