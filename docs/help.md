# elasticsearch-query-toolkit 使用手册

---
### 一、SQL基本结构
```bash
## 搜索请求SQL结构
SELECT [fields] FROM [index.type] QUERY [score query condition] WHERE [bool query condition] ORDER BY [sort field] LIMIT [0, 10]

## 聚合查询SQL结构
SELECT [MIN(field),MAX(field),SUM(field),AVG(field)] from [index.type] QUERY [score query condition] WHERE [bool query condition] GROUP BY [agg methods]
```

以上两种SQL结构分别对应ES的搜索请求和聚合请求，下面大概看看这两种结构，后续会详细介绍。
1、搜索请求SQL

 - SELECT项，可以是通配符`*` 也可以是具体指定需要返回的字段（针对inner/nested文档直接通过`.`引用）
 - FROM项，后面跟的是索引名和文档类型如：product.car 表示查询product这个索引下的car类型
 - QUERY项，关键字QUERY和WHERE是同级的，不同之处是QUERY后面跟的查询条件会进行`打分`
 - WHERE项，和QUERY同级，后面跟的查询条件不会参与打分，只是进行简单的`bool匹配`
 - ORDER BY项，跟SQL一样后面跟排序条件
 - LIMIT 项，指定分页参数对应ES的from，size参数

2、 聚合请求SQL

 - SELECT/FROM/QUERY/WHERE 项同上
 - GROUP BY项，后面跟的是需要分组查询的条件，如前支持只支持：terms、range

--------

为了能更好的说明SQL语法的使用，我们假设已经存在一个名叫`product`的索引，索引下有一个`apple`的类型，具体的mapping像下面这样，其中provider是一个inner doc表示供应商，buyers是一个nested doc表示购买者。
```bash
"mappings": {
	"apple": {
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
```

### 二、搜索请求SQL查询语法
**注意：由于Inner Doc和Root Doc的对应关系是一对一，Nested Doc和Root Doc的对应关系是多对一，所以为了区分这种关系在引用Nesetd Doc时需要在类型前加上 \$ 符号，如：\$buyers.buyerName(Nested Doc引用需要加 \$ 符号)，provider.privideName(Inner Doc直接引用)**

#### 1. SELECT的使用
```bash
# 支持通配符
SELECT * FROM product.apple

# 指定具体返回的字段
SELECT productName，productCode FROM product.apple

# 对Inner Doc字段的引用直接通过 “.”
SELECT provider.providerName FROM product.apple
# 当然也可对Inner Doc字段通配
SELECT productName，provider.* FROM product.apple

# 对于Nested Doc的引用需要需要在类型名前加上 “$”
SELECT $buyers.buyerName FROM product.apple
```

#### 2. WHERE条件的使用
在SQL中WHERE项后面指定的是bool条件，不会参与打分，这些条件最终会被放到`bool query`中的`filter`中提交给ES指定

##### 1) 基本条件支持 <、>、<=、>=、between...and、in、not in、is null、not null
```bash
SELECT * FROM product.apple WHERE minPrice >= 100 and minPrice <= 200
SELECT * FROM product.apple WHERE minPrice BETWEEN 100 AND 200

SELECT * FROM product.apple WHERE provider.providerLevel in (1, 2, 3) and provider.providerLevel not in (4, 5)

SELECT * FROM product.apple WHERE productName is not null and productCode is null
```

**注意：上面SQL中`WHERE`可以完全替换成`QUERY`返回的结果一模一样，唯一不同的是`QUERY`会计算得分，`WHERE`只是单纯的bool匹配**


##### 2) 内嵌查询patch_context自动识别
我们都知道在针对nested doc做查询时有专门的nest查询语法，且必须指定一个nested_path才能将多个子查询放到同一个nested query context中，在使用SQL对nested doc查询时不需要显示指定某个或多个条件是存在同一个context中，程序会自动识别，看下面例子:

两个条件同级都是针对内嵌文档的，会被识别存在于同一个neste context里
```bash
SELECT * FROM product.apple WHERE $buyers.buyerName = 'usa' and $buyers.productPrice < 200
{
  "query" : {
    "bool" : {
      "filter" : {
        "bool" : {
          "must" : {
            "nested" : {
              "query" : {
                "bool" : {
                  "must" : [ {
                    "term" : {
                      "buyers.buyerName" : "usa"
                    }
                  }, {
                    "range" : {
                      "buyers.productPrice" : {
                        "from" : null,
                        "to" : 200,
                        "include_lower" : true,
                        "include_upper" : false
                      }
                    }
                  } ]
                }
              },
              "path" : "buyers"
            }
          }
        }
      }
    }
  }
}
```
两个内嵌条件不同级放到不同nested context里面 

```bash
SELECT * FROM product.apple WHERE ($buyers.buyerName = 'usa' or minPrice > 100)  and $buyers.productPrice < 200

{
  "query" : {
    "bool" : {
      "filter" : {
        "bool" : {
          "must" : [ {
            "bool" : {
              "should" : [ {
                "range" : {
                  "minPrice" : {
                    "from" : 100,
                    "to" : null,
                    "include_lower" : false,
                    "include_upper" : true
                  }
                }
              }, {
                "nested" : {
                  "query" : {
                    "term" : {
                      "buyers.buyerName" : "usa"
                    }
                  },
                  "path" : "buyers"
                }
              } ]
            }
          }, {
            "nested" : {
              "query" : {
                "range" : {
                  "buyers.productPrice" : {
                    "from" : null,
                    "to" : 200,
                    "include_lower" : true,
                    "include_upper" : false
                  }
                }
              },
              "path" : "buyers"
            }
          } ]
        }
      }
    }
  }
}

```
##### 3) Inner Doc查询直接引用即可
```bash
SELECT * FROM product.apple WHERE provider.providerLevel in (1, 2, 3)
SELECT * FROM product.apple WHERE provider.providerName = 'usa'
```
##### 4) term 查询
```bash
# 在WHERE条件中指定仅仅作为bool查询
SELECT * FROM product.apple WHERE term(productName, 'iphone6s')

{
  "query" : {
    "bool" : {
      "filter" : {
        "bool" : {
          "must" : {
            "term" : {
              "productName" : "iphone6s"
            }
          }
        }
      }
    }
  }
}

# 在QUERY条件中指定根据文档打分排序，并能指定可选参数， 指定权重
SELECT * FROM product.apple QUERY term(productName, 'iphone6s', 'boost:2.0f')
{
  "query" : {
    "bool" : {
      "must" : {
        "term" : {
          "productName" : {
            "value" : "iphone6s",
            "boost" : 2.0
          }
        }
      }
    }
  }
}
```
**注意1：这里的可选参数是一个以key:value组成的键值对，多个键值对之间用逗号隔开**
**注意2：terms查询和term类似，这里不再赘述**
**注意3：term查询其实和SQL中的 “=” 操作一样**

##### 5) match query使用
```bash
SELECT * FROM product.apple QUERY match(productName, 'iphone6s', 'boost:2.0f,type:boolean,operator:or,minimum_should_match:75%') WHERE minPrice > 100

{
  "query" : {
    "bool" : {
      "must" : {
        "match" : {
          "productName" : {
            "query" : "iphone6s",
            "type" : "boolean",
            "operator" : "OR",
            "boost" : 2.0,
            "minimum_should_match" : "75%"
          }
        }
      },
      "filter" : {
        "bool" : {
          "must" : {
            "range" : {
              "minPrice" : {
                "from" : 100,
                "to" : null,
                "include_lower" : false,
                "include_upper" : true
              }
            }
          }
        }
      }
    }
  }
}
```
**注意1：这里使用了QUERY和WHERE两种条件配合使用, WHERE达到过滤的效果, QUERY在过滤结果基础上再进行match匹配**
**注意2： match查询所有的可选参数请参看官网的参数说明：https://www.elastic.co/guide/en/elasticsearch/reference/2.4/query-dsl-match-query.html**
**注意3：match查询支持多个函数名：match、match_query、matchQuery**

##### 6) multi match使用
```bash
SELECT * FROM product.apple QUERY multiMatch('productName^3, productCode', 'iphone6s') WHERE minPrice > 100

{
  "query" : {
    "bool" : {
      "must" : {
        "multi_match" : {
          "query" : "iphone6s",
          "fields" : [ "productName^3", " productCode" ]
        }
      },
      "filter" : {
        "bool" : {
          "must" : {
            "range" : {
              "minPrice" : {
                "from" : 100,
                "to" : null,
                "include_lower" : false,
                "include_upper" : true
              }
            }
          }
        }
      }
    }
  }
}
```

##### 7) 
```bash
SELECT * FROM product.apple QUERY  WHERE minPrice > 100
```