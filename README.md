SQL to DSL for Elasticsearch
====================================

elasticsearch-sql2dsl版本
-------------

sql2dsl version | ES version
-----------|-----------
master | 1.4.5

sql2dsl介绍
-------------
> `elasticsearch-sql2dsl`是一款能将SQL转换为ES的查询语言DSL的工具包，目前只支持比较简单的查询。


使用注意
------
> * 在SQL的where条件中，原子条件的左边必须为变量名，右边必须为查询参数值
> * 针对`Inner Doc`，直接引用即可：`a.b.c.d`
> * 针对`Nested Doc`，需要在内嵌文档加上`$`符号： `$b.c`

## Api使用示例

常规sql查询，不带参数
```java
String sql = "select * from index.order where status='SUCCESS' order by nvl(pride, 0) asc limit 0, 20";

ElasticSql2DslParser sql2DslParser = new ElasticSql2DslParser();
//解析SQL
ElasticSqlParseResult parseResult = sql2DslParser.parse(sql);

//生成DSL,可用于rest api调用
String dsl = parseResult.toDsl();

//toRequest方法接收一个clinet对象参数，用于生成SearchRequestBuilder
SearchRequestBuilder searchReq = parseResult.toRequest(esClient);

//执行查询
SearchResponse response = searchReq.execute().actionGet();
```
指定可变参数查询
```java
String sql = "select * from index.order where status=? order by nvl(pride, 0) asc limit ?, ?";

ElasticSql2DslParser sql2DslParser = new ElasticSql2DslParser();
//指定参数，解析SQL
ElasticSqlParseResult parseResult = sql2DslParser.parse(sql, new Object[]{"SUCCESS", 0, 20});

//生成DSL,可用于rest api调用
String dsl = parseResult.toDsl();

//toRequest方法接收一个clinet对象参数，用于生成SearchRequestBuilder
SearchRequestBuilder searchReq = parseResult.toRequest(esClient);

//执行查询
SearchResponse response = searchReq.execute().actionGet();
```

使用WHERE条件解析时的回调方法
```java
String sql = "select * from index.order where status='SUCCESS' and lastUpdateTime > '2017-01-02' order by nvl(pride, 0) asc limit 0, 20";

ElasticSql2DslParser sql2DslParser = new ElasticSql2DslParser();

//指定SQL解析监听器,解析SQL
ElasticSqlParseResult parseResult = sql2DslParser.parse(sql, new ParseActionListenerAdapter() {
    @Override
    public void onAtomConditionParse(ElasticSqlQueryField paramName, Object[] paramValues, SQLConditionOperator operator) {
        if(paramName.getQueryFieldType() == QueryFieldType.RootDocField && "lastUpdateTime".equals(paramName.getQueryFieldFullName())) {
            //这里是解析SQL中原子条件时的回调方法
            //某些按时间划分的索引,可在此解析出SQL中指定的时间返回,并重新设置需要查询的索引
        }
    }
});

//生成DSL,可用于rest api调用
String dsl = parseResult.toDsl();

//toRequest方法接收一个clinet对象参数，用于生成SearchRequestBuilder
SearchRequestBuilder searchReq = parseResult.toRequest(esClient);

//执行查询
SearchResponse response = searchReq.execute().actionGet();

```
使用routing by指定routing参数
```bash
String sql = "select * from index.order where status='SUCCESS' order by nvl(pride, 0) asc routing by 'A','B' limit 0, 20";

ElasticSql2DslParser sql2DslParser = new ElasticSql2DslParser();
//解析SQL，
ElasticSqlParseResult parseResult = sql2DslParser.parse(sql);

//toRequest方法会把指定的routing参数放到SearchRequest中
SearchRequestBuilder searchReq = parseResult.toRequest(esClient);
```


## SQL使用示例
```bash
#下面index表示索引名，order表示文档类型名
select * from index.order

# 1. 默认从第0条数据开始，取15条
# 2. 因为没有带where条件所以查询是match_all
{
  "from" : 0,
  "size" : 15,
  "query" : {
    "match_all" : { }
  }
}

# 带上分页参数查询
select * from index.order limit 0,100

# 1. 分页参数从0开始取100条
{
  "from" : 0,
  "size" : 100,
  "query" : {
    "match_all" : { }
  }
}

# where条件中带一个status参数
select * from index.order where status='SUCCESS' limit 0,100

{
  "from" : 0,
  "size" : 100,
  "query" : {
    "filtered" : {
      "filter" : {
        "bool" : {
          "must" : {
            "term" : {
              "status" : "SUCCESS"
            }
          }
        }
      }
    }
  }
}

# totalPrice 范围查询
select * from index.order where status='SUCCESS' and totalPrice > 1000 limit 0,100

{
  "from" : 0,
  "size" : 100,
  "query" : {
    "filtered" : {
      "filter" : {
        "bool" : {
          "must" : [ {
            "term" : {
              "status" : "SUCCESS"
            }
          }, {
            "range" : {
              "totalPrice" : {
                "from" : 1000,
                "to" : null,
                "include_lower" : false,
                "include_upper" : true
              }
            }
          } ]
        }
      }
    }
  }
}

# between...and... 上下限都取
select * from index.order where status='SUCCESS' and totalPrice between 1000 and 2000 limit 0,100

{
  "from" : 0,
  "size" : 100,
  "query" : {
    "filtered" : {
      "filter" : {
        "bool" : {
          "must" : [ {
            "term" : {
              "status" : "SUCCESS"
            }
          }, {
            "range" : {
              "totalPrice" : {
                "from" : 1000,
                "to" : 2000,
                "include_lower" : true,
                "include_upper" : true
              }
            }
          } ]
        }
      }
    }
  }
}

# 日期范围查询，下面3条SQL等效
select * from index.order where status='SUCCESS' and lastUpdateTime > '2017-01-01 00:00:00' limit 0,100
select * from index.order where status='SUCCESS' and lastUpdateTime > '2017-01-01' limit 0,100
#日期函数可以自定义日期格式
select * from index.order where status='SUCCESS' and lastUpdateTime > date('yyyy-MM-dd', '2017-01-01') limit 0,100

{
  "from" : 0,
  "size" : 100,
  "query" : {
    "filtered" : {
      "filter" : {
        "bool" : {
          "must" : [ {
            "term" : {
              "status" : "SUCCESS"
            }
          }, {
            "range" : {
              "lastUpdateTime" : {
                "from" : "2017-01-01T00:00:00.000+0800",
                "to" : null,
                "include_lower" : false,
                "include_upper" : true
              }
            }
          } ]
        }
      }
    }
  }
}

# 排序条件，price升序，publishDate降序
select * from index.order where status='SUCCESS' order by price asc, publishDate desc

{
  "from" : 0,
  "size" : 15,
  "query" : {
    "filtered" : {
      "filter" : {
        "bool" : {
          "must" : {
            "term" : {
              "status" : "SUCCESS"
            }
          }
        }
      }
    }
  },
  "sort" : [ {
    "price" : {
      "order" : "asc"
    }
  }, {
    "publishDate" : {
      "order" : "desc"
    }
  } ]
}

# 使用nvl函数指定默认值
select * from index.order where status='SUCCESS' order by nvl(price,0) asc, publishDate desc

{
  "from" : 0,
  "size" : 15,
  "query" : {
    "filtered" : {
      "filter" : {
        "bool" : {
          "must" : {
            "term" : {
              "status" : "SUCCESS"
            }
          }
        }
      }
    }
  },
  "sort" : [ {
    "price" : {
      "order" : "asc",
      "missing" : 0
    }
  }, {
    "publishDate" : {
      "order" : "desc"
    }
  } ]
}

#内嵌文档排序，指定sort_mode
select * from index.order where status='SUCCESS' order by nvl(product.$providers.sortNo, 0, 'min') asc, publishDate desc

{
  "from" : 0,
  "size" : 15,
  "query" : {
    "filtered" : {
      "filter" : {
        "bool" : {
          "must" : {
            "term" : {
              "status" : "SUCCESS"
            }
          }
        }
      }
    }
  },
  "sort" : [ {
    "product.providers.sortNo" : {
      "order" : "asc",
      "missing" : 0,
      "mode" : "min",
      "nested_path" : "product.providers"
    }
  }, {
    "publishDate" : {
      "order" : "desc"
    }
  } ]
}

# Inner Doc 查询
select * from index.order where seller.name='JD' order by id desc

{
  "from" : 0,
  "size" : 15,
  "query" : {
    "filtered" : {
      "filter" : {
        "bool" : {
          "must" : {
            "term" : {
              "seller.name" : "JD"
            }
          }
        }
      }
    }
  },
  "sort" : [ {
    "id" : {
      "order" : "desc"
    }
  } ]
}


#Nested Doc查询
select * from index.order where product.$providers.name in ('JD', 'TB') and product.price < 1000 order by id desc

{
  "from" : 0,
  "size" : 15,
  "query" : {
    "filtered" : {
      "filter" : {
        "bool" : {
          "must" : [ {
            "range" : {
              "product.price" : {
                "from" : null,
                "to" : 1000,
                "include_lower" : true,
                "include_upper" : false
              }
            }
          }, {
            "nested" : {
              "filter" : {
                "terms" : {
                  "product.providers.name" : [ "JD", "TB" ]
                }
              },
              "path" : "product.providers"
            }
          } ]
        }
      }
    }
  },
  "sort" : [ {
    "id" : {
      "order" : "desc"
    }
  } ]
}

#组合条件查询
select * from index.order where (product.$providers.name in ('JD', 'TB') or product.$providers.channel='ONLINE') and (product.status='OPEN' or product.price < 1000) order by id desc

{
  "from" : 0,
  "size" : 15,
  "query" : {
    "filtered" : {
      "filter" : {
        "bool" : {
          "must" : [ {
            "bool" : {
              "should" : {
                "nested" : {
                  "filter" : {
                    "bool" : {
                      "should" : [ {
                        "terms" : {
                          "product.providers.name" : [ "JD", "TB" ]
                        }
                      }, {
                        "term" : {
                          "product.providers.channel" : "ONLINE"
                        }
                      } ]
                    }
                  },
                  "path" : "product.providers"
                }
              }
            }
          }, {
            "bool" : {
              "should" : [ {
                "term" : {
                  "product.status" : "OPEN"
                }
              }, {
                "range" : {
                  "product.price" : {
                    "from" : null,
                    "to" : 1000,
                    "include_lower" : true,
                    "include_upper" : false
                  }
                }
              } ]
            }
          } ]
        }
      }
    }
  },
  "sort" : [ {
    "id" : {
      "order" : "desc"
    }
  } ]
}

# 查询字段
select totalPrice, product.*, product.$seller.* from index.order

{
  "from" : 0,
  "size" : 15,
  "query" : {
    "match_all" : { }
  },
  "_source" : {
    "includes" : [ "totalPrice", "product.*", "product.seller.*" ],
    "excludes" : [ ]
  }
}
```




## 聚合统计

```bash
select min(price),avg(price) from index.product group by terms(category),terms(color),range(price, segment(0,100), segment(100,200), segment(200,300))


{
  "from" : 0,
  "size" : 15,
  "query" : {
    "match_all" : { }
  },
  "aggregations" : {
    "agg_category" : {
      "terms" : {
        "field" : "category",
        "size" : 500,
        "shard_size" : 1000,
        "min_doc_count" : 1,
        "shard_min_doc_count" : 1,
        "order" : {
          "_count" : "desc"
        }
      },
      "aggregations" : {
        "agg_color" : {
          "terms" : {
            "field" : "color",
            "size" : 500,
            "shard_size" : 1000,
            "min_doc_count" : 1,
            "shard_min_doc_count" : 1,
            "order" : {
              "_count" : "desc"
            }
          },
          "aggregations" : {
            "agg_price" : {
              "range" : {
                "field" : "price",
                "ranges" : [ {
                  "key" : "0-100",
                  "from" : 0.0,
                  "to" : 100.0
                }, {
                  "key" : "100-200",
                  "from" : 100.0,
                  "to" : 200.0
                }, {
                  "key" : "200-300",
                  "from" : 200.0,
                  "to" : 300.0
                } ]
              },
              "aggregations" : {
                "min_price" : {
                  "min" : {
                    "field" : "price"
                  }
                },
                "avg_price" : {
                  "avg" : {
                    "field" : "price"
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

```

作者:  [@陈楠][1]
Email: 465360798@qq.com

<完>

