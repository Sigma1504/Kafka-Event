package com.accor.tech.domain;

import com.fasterxml.jackson.annotation.JsonInclude;
import lombok.*;
import lombok.experimental.Wither;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;

@Getter
@Setter
@Wither
@AllArgsConstructor
@NoArgsConstructor
@Builder
@ToString
public class EventActionStream {
    private String streamName;
    @JsonInclude(JsonInclude.Include.NON_NULL)
    private TypeAction typeAction;
    @JsonInclude(JsonInclude.Include.NON_NULL)
    private Date dateGeneration;
    @JsonInclude(JsonInclude.Include.NON_NULL)
    private String instanceName;
    @JsonInclude(JsonInclude.Include.NON_NULL)
    private TypeStatus eventStatus;
    @JsonInclude(JsonInclude.Include.NON_NULL)
    private String status;
    @JsonInclude(JsonInclude.Include.NON_NULL)
    private HashMap<String, String> mapLabels;
    @JsonInclude(JsonInclude.Include.NON_NULL)
    private Map<String, Double> mapMetrics;
}
