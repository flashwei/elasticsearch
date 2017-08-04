package com.elasticsearch.es.service;

import java.util.List;

import com.elasticsearch.es.dto.BaseEsDto;

public interface IBaseEsService {
	String indexOne(BaseEsDto baseEsDto);

	<T extends BaseEsDto> void indexList(List<T> baseEsDtoList);

	<T extends BaseEsDto> T getById(String id, Class<T> tClass);

	boolean deleteById(String id);

	boolean updateById(String id, BaseEsDto baseEsDto);
}
