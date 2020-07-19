package com.barry.kafka.bean;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @author boruiShao
 * Date 2020/7/18 2:57 PM
 * Version 1.0
 * Describe TODO
 **/

@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class Company {
    private String name;
    private String address;
}
