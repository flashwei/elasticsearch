package com.elasticsearch.es.dto;

import com.alibaba.fastjson.JSON;

/**
 * @Author: Vachel Wang
 * @Date: 2017/6/1下午7:05
 * @Version: V1.0
 * @Description: ES 数据传输对象
 */
public abstract class BaseEsDto {
    // id 为空自动生成
    private String id ;

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String toJsonString(){
        return JSON.toJSONString(this) ;
    }
}
