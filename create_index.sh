#!/usr/bin/env bash
curl -XDELETE 'http://192.168.0.109:9200/index/'

curl -XPUT 'http://192.168.0.109:9200/index/' -d '{
	"settings": {
		"index": {
			"number_of_shards": 1,
			"number_of_replicas": 0
		}
	},
	"mappings": {
		"product": {
			"properties": {
				"productName": {
					"type": "string",
					"index": "not_analyzed"
				},
				"productCode": {
					"type": "string",
					"index": "not_analyzed"
				},
				"minPrice": {
					"type": "double"
				},
				"advicePrice": {
					"type": "double"
				},
				"provider": {
					"type": "object",
					"properties": {
						"providerName": {
							"type": "string",
							"index": "not_analyzed"
						},
						"providerLevel" : {
							"type": "integer"
						}
					}
				},
				"buyers": {
					"type": "nested",
					"properties": {
						"buyerName": {
							"type": "string",
							"index": "not_analyzed"
						},
						"productPrice": {
							"type": "double"
						}
					}
				}
			}
		}
	}
}'


curl -XPUT 'http://192.168.0.109:9200/index/product/1' -d '{
	"productName" : "iphone 6s",
	"productCode" : "IP_6S",
	"minPrice" : 2288.00,
	"advicePrice" : "6288.00",
	"provider" : {
		"providerName" : "foxconn",
		"providerLevel" : 1
	},
	"buyers" : [{
		"buyerName" : "china",
		"productPrice" : 9288.00
	},
	{
		"buyerName" : "usa",
		"productPrice" : 3288.00
	},
	{
		"buyerName" : "japan",
		"productPrice" : 4288.00
	}]
}'

curl -XPUT 'http://192.168.0.109:9200/index/product/2' -d '{
	"productName" : "apple watch os2",
	"productCode" : "AW_OS2",
	"minPrice" : 1000.00,
	"advicePrice" : "5000.00",
	"provider" : {
		"providerName" : "foxconn",
		"providerLevel" : 1
	},
	"buyers" : [{
		"buyerName" : "china",
		"productPrice" : 9999.00
	},
	{
		"buyerName" : "usa",
		"productPrice" : 4500.00
	},
	{
		"buyerName" : "japan",
		"productPrice" : 6000.00
	}]
}'

curl -XPOST 'http://192.168.0.109:9200/index/_refresh'