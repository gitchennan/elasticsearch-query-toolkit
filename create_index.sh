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
		"library": {
			"properties": {
				"name": {
					"type": "string",
					"index": "not_analyzed"
				},
				"manager": {
					"type": "object",
					"properties": {
						"managerName": {
							"type": "string",
							"index": "not_analyzed"
						},
						"floors": {
							"type": "nested",
							"properties": {
								"floorNum": {
									"type": "integer"
								},
								"area": {
									"type": "string",
									"index": "not_analyzed"
								}
							}
						}
					}
				},
				"bookCategories": {
					"type": "nested",
					"properties": {
						"categoryName": {
							"type": "string",
							"index": "not_analyzed"
						},
						"categoryCode": {
							"type": "string",
							"index": "not_analyzed"
						},
						"books": {
							"type": "nested",
							"properties": {
								"bookStock": {
									"type": "integer"
								},
								"bookAuthor": {
									"type": "string",
									"index": "not_analyzed"
								},
								"bookName": {
									"type": "string",
									"index": "not_analyzed"
								},
								"bookPublisher" : {
								    "type" : "object",
								    "properties" : {
								        "publisherName" : {
								            "type" : "string",
								            "index": "not_analyzed"
								        },
								        "publisherCode" : {
								            "type" : "string",
								            "index": "not_analyzed"
								        },
								        "bookProvider" : {
                                            "type" : "nested",
                                            "properties" : {
                                                "providerName" : {
                                                    "type" : "string",
								                    "index": "not_analyzed"
                                                }
                                            }
                                        }
                                    }
								}
							}
						}
					}
				}
			}
		}
	}
}'


curl -XPUT 'http://192.168.0.109:9200/index/library/1' -d '{
	"name": "HBUT",
	"manager": {
		"managerName": "CN",
		"floors": [{
			"floorNum": 1,
			"area": ["A","B"]
		},
		{
			"floorNum": 2,
			"area": ["A","B","C"]
		}]
	},
	"bookCategories": [{
		"categoryName": "IT",
		"categoryCode": "C001",
		"books": [{
			"bookName": "Java Core",
			"bookStock": 22,
			"bookAuthor": "jason"
		},
		{
			"bookName": "Multi Thread",
			"bookStock": 12,
			"bookAuthor": "jason"
		}]
	},
	{
		"categoryName": "ART",
		"categoryCode": "C002",
		"books": [{
			"bookName": "Chinese 5000",
			"bookStock": 13,
			"bookAuthor": "bibic"
		},
		{
			"bookName": "qgjq",
			"bookStock": 18,
			"bookAuthor": "lcy"
		}]
	}]
}'



curl -XPUT 'http://192.168.0.109:9200/index/library/2' -d '{
	"name" : "HZKJDX",
	"manager" : {
		"managerName": "lcy",
		"floors": [{
			"floorNum" : 1,
			"area" : ["M", "X"]
		},
		{
			"floorNum" : 2,
			"area" : ["A"]
		},
		{
			"floorNum" : 4,
			"area" : ["N"]
		}]
	},
	"bookCategories" : [{
		"categoryName" : "NEWS",
		"categoryCode" : "C001",
		"books" : [{
			"bookName" : "cqcb",
			"bookStock" : 22,
			"bookAuthor" : "cq"
		},
		{
			"bookName" : "yyxw",
			"bookStock" : 12,
			"bookAuthor" : "cq"
		}]
	},
	{
		"categoryName" : "ART",
		"categoryCode" : "C002",
		"books" : [{
			"bookName" : "Cinese 5000",
			"bookStock" : 13,
			"bookAuthor" : "bibicx",
			"bookPublisher" : {
			    "publisherName" : "CQ_PUB",
			    "publisherCode" : "PUB_03",
			    "bookProvider" : [{
                    "providerName" : "PVD_01"
                }]
			}
		},
		{
			"bookName" : "qgjq",
			"bookStock" : 18,
			"bookAuthor" : "lcy"
		}]
	}]
}'


curl -XPOST 'http://192.168.0.109:9200/index/_refresh'



curl -XPOST 'http://192.168.0.109:9200/index/_search' -d '{
  "query": {
    "filtered": {
      "filter": {
        "nested": {
          "path": "bookCategories",
          "filter": {
            "bool": {
              "must": [
                {
                  "term": {
                    "bookCategories.categoryName": "ART"
                  }
                }
              ]
            }
          }
        }
      }
    }
  }
}'


curl -XPOST 'http://192.168.0.109:9200/index/_search' -d '{
  "query": {
    "filtered": {
      "filter": {
        "nested": {
          "path": "manager.floors",
          "filter": {
            "bool": {
              "must": [
                {
                  "term": {
                    "manager.floors.floorNum": 1
                  }
                }
              ]
            }
          }
        }
      }
    }
  }
}'


curl -XPOST 'http://192.168.0.109:9200/index/_search' -d '{
  "query": {
    "filtered": {
      "filter": {
        "nested": {
          "filter": {
            "bool": {
              "must": [
                {
                  "term": {
                    "bookCategories.categoryName": "ART"
                  }
                },
                {
                  "nested": {
                    "filter": {
                      "bool": {
                        "must": {
                          "terms": {
                            "bookCategories.books.bookAuthor": [
                              "bibicx"
                            ]
                          }
                        }
                      }
                    },
                    "path": "bookCategories.books"
                  }
                }
              ]
            }
          },
          "path": "bookCategories"
        }
      }
    }
  }
}'

