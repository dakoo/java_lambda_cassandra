package com.example;

import com.example.annotations.PartitionKey;
import com.example.annotations.VersionKey;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import lombok.*;

import java.util.List;
import java.util.Map;

@Data
@NoArgsConstructor
@JsonIgnoreProperties(ignoreUnknown = true)
public class Dish {
    @PartitionKey
    private Long id;
    @VersionKey
    private Long version;
    private Long storeId;
    private Map<String, String> names;
    private Map<String, String> descriptions;
    private String taxBaseType;
    private String displayStatus;
    private String targetAvailableTime;
    private Double salePrice;
    private String currencyType;
    private List<String> imagePaths;
    private String saleFromAt;
    private String saleToAt;
    private List<DishOption> dishOptions;
    private List<DishOpenHour> openHours;
    private Boolean disposable;
    private Double disposablePrice;
    private Boolean deleted;
    private Double displayPrice;
}
