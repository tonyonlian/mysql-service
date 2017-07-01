const mysql = require('mysql2');
const  async =  require('async');
const Joi = require('joi');
const Hoek = require('hoek');

const INSET = 'inset';
const QUERY = 'query';
const UPDATE = 'update';
const REMOVE = 'remove';
const PAGE_COUNT = 'pageCount';

var pool = '';


/**
 * MySQL 增，删，查，改 事务方法的封装
 */

/**
 * 插入方法
 * @param {*} tableName ：表名
 * @param {*} data ：参数列表
 */

function insert(tableName,data){
   let sqlParamMap = _constructInsertSqlParamMap(tableName,data);
   return _exeSql(sqlParamMap.sql,sqlParamMap.params,sqlParamMap.type);

}

/**
 * 查询方法
 * @param {*} sql ：查询SQL语句
 * @param {*} args ：参数列表
 */

function query(sql,...args){

   let sqlParamMap = _constructQuerySqlParamMap(sql,args);
   return _exeSql(sqlParamMap.sql,sqlParamMap.params,sqlParamMap.type);

} 

/**
 * 更新方法
 * @param {*} tableName ：表名
 * @param {*} id ：记录id
 * @param {*} data ：参数列表
 */

function update(tableName,id,data){
   let sqlParamMap = _constructUpdateSqlParamMap(tableName,id,data);
   return _exeSql(sqlParamMap.sql,sqlParamMap.params,sqlParamMap.type);

}

/**
 * 删除方法
 * @param {*} tableName ：表名
 * @param {*} id ：表记录的id
 */

function remove(tableName,id){
    let sqlParamMap = _constructDelSqlParamMap(tableName,id);
    return _exeSql(sqlParamMap.sql,sqlParamMap.params,sqlParamMap.type);
    
}



/**
 * 分页查询
 * @param {*} tableName ：表名
 * @param {*} pageParam ：分页参数 eg {pageNo:2,pageSize:10}
 * @param {*} args ：查询参数 
 * 
 */

  const pagesShema = Joi.object().keys({
    pageNo:Joi.number().integer().required(),
    pageSize:Joi.number().integer().required()
  })


  function queryListPage(sql,pageParam,...args){ 
   
     const result = Joi.validate(pageParam, pagesShema);
     if(result.error){
        reject(result.error);
        return;
     }
    let sqlparamsEntities =  _constructPageSqlParamArr(sql,pageParam,...args);
    let funcAry = [];
    sqlparamsEntities.forEach(function(obj) {
        let _sql = obj.sql;
        let params = obj.params;
        var _temp =function(callback){
            query(_sql,params).then(function(data){
                callback(null,data);
            }).catch(function(err){
                callback(err);
            })
        }
        funcAry.push(_temp);  
    });

    return new Promise(function(resolve,reject){
         async.series(funcAry,function(err,result){
             if(err){
                 reject(err);
                 return;
             }
              pageParam.count = result[0][0].total;
              result[0] = pageParam;
              resolve(result)
         });

    });

}


/**
 * MySQL事务
 * @param {*} handles ：函数参数
 * 
 */
function trans(handles){
    var _transContext = {
        _sqlMapping:[],
        _pageParams :{},
        insert:function(tableName,data) {
            let sqlParamMap = _constructInsertSqlParamMap(tableName,data);
            this._sqlMapping.push(sqlParamMap)
           
        },
        query:function(sql,...args) {
            let sqlParamMap = _constructQuerySqlParamMap(sql,args);
            this._sqlMapping.push(sqlParamMap)
        },
        update:function(tableName,id,data) {
            let sqlParamMap = _constructUpdateSqlParamMap(tableName,id,data);
            this._sqlMapping.push(sqlParamMap);

        },
        remove:function(tableName,id) {
            let sqlParmMap = _constructDelSqlParamMap(tableName,id);
            this._sqlMapping.push(sqlParmMap);

        },
        queryListPage:function(sql,pageParam,...args) {
            let sqlParmArr = _constructPageSqlParamArr(sql,pageParam,...args);
            pageParam.count = 0;
            this._pageParams = pageParam;
            this._sqlMapping.push(sqlParmArr[0]);
            this._sqlMapping.push(sqlParmArr[1]);

        },
        sqlHandler:handles
    };
   
    if(handles){
        _transContext.sqlHandler();
    }

    return new Promise(function(resolve,reject){
        _execTrans(_transContext._sqlMapping,_transContext._pageParams,function(err,result){
             if(err){
                 reject(err);
                 return;
             }
             resolve(result)
        });

    });
}





function _execTrans(sqlparamsEntities,pageParam,callback){
    pool.getConnection(function(err,connection){
        if(err){
            return callback(err);
        }
        connection.beginTransaction(function(err){
            if(err){
                return callback(err);
            }
            console.log("开始执行transaction,共执行" +sqlparamsEntities.length +"数据条数");
            var funcAry = [];
            sqlparamsEntities.forEach(function(sql_param) {
               var temp = function(cb){
                   var sql = sql_param.sql;
                   var param = sql_param.params;
                   let type = sql_param.type;
                   connection.query(sql,param,function(tErr,result,fields){
                       if(tErr){
                           connection.rollback(function(){
                                 console.log("事务失败，" + sql_param + "，ERROR：" + tErr);
                                 throw tErr;
                           });
                       }else{
                           let data = '';
                            switch(type){
                                case INSET:
                                   data = {id:result.insertId}
                                break;
                                case UPDATE:
                                   data = {affectedRows:result.affectedRows};
                                break;
                                case PAGE_COUNT:
                                   pageParam.count = result[0].total;
                                   data = pageParam;
                                break;
                                 case REMOVE:
                                   data = {affectedRows:result.affectedRows};
                                break;
                                default:
                                   data = result
                            }
                           return cb(null, data);
                       }
                   });
               }
               funcAry.push(temp);  
            });

            async.series(funcAry,function(err,result){
                 console.log("transaction error: " + err);
                 if(err){
                        connection.rollback(function (err) {
                        console.log("transaction error: " + err);
                        connection.release();
                        return callback(err, null);
                    });
                 }else{
                     connection.commit(function(err,info){
                        console.log("transaction info: " + JSON.stringify(info));
                        if(err){
                            console.log("执行事务失败，" + err);
                            connection.rollback(function (err) {
                                console.log("transaction error: " + err);
                                connection.release();
                                return callback(err, null);
                            });
                        }else{
                            connection.release();
                            return callback(null, result)
                        }

                     });

                 }

            });


        });
    });
}



function  _getNewSqlParamEntity(sql, params,type, callback) {

  if(callback){
      return callback(null,{
            sql: sql,
            params: params,
            type:type
      })
  }
  return {
        sql: sql,
        params: params,
        type:type
    };

}

function  _exeSql(sql,params,type){
      return new Promise(function(resolve,reject){
            pool.getConnection(function(err, connection) {
                if(err){
                    reject(err);
                    connection.destroy();
                    return console.error(err);
                }else{
                    connection.query(sql,params,function(err,result,fields){
                        if(err){
                            reject(err);
                        }else{
                            let data = '';
                            switch(type){
                                case INSET:
                                   data = {id:result.insertId}
                                break;
                                case UPDATE:
                                   data = {affectedRows:result.affectedRows};
                                break;
                                case REMOVE:
                                   data = {affectedRows:result.affectedRows};
                                break;
                                default:
                                   data = result
                            }
                            resolve(data);
                        }
                        connection.release();
                    });
                }
            });
    });
      
}
 
function _constructInsertSqlParamMap(tableName,data) {
    var sql = `INSERT INTO ${tableName} set?`;
    var param = data;
    return _getNewSqlParamEntity(sql,param,INSET);
}

function _constructQuerySqlParamMap(sql,data) {
    return _getNewSqlParamEntity(sql,data,QUERY);
}

function _constructUpdateSqlParamMap(tableName,id,data) {
    var _args = [];
    var _setArr = [];
    for(var key in data ){
        let set = key+"=? "
        _setArr.push(set);
        _args.push(data[key]);
    }
    _args.push(id);
    var setSql = _setArr.join(",");
    var updSql = `UPDATE ${tableName} SET ${setSql} where id=?`;
    return _getNewSqlParamEntity(updSql,_args,UPDATE);
}

function _constructDelSqlParamMap(tableName,id) {
    var delSql = `delete from ${tableName} where id=?`;
    var param = id;
    return _getNewSqlParamEntity(delSql,param,REMOVE);
}

function _constructPageSqlParamArr(sql,pageParam,...args) {
    let sqlLower = sql.toLocaleLowerCase();
    let limitFirtParam = (pageParam.pageNo-1) * pageParam.pageSize;
    let limitSecParam = pageParam.pageSize;
    let sqlPart = sql.substr(sqlLower.indexOf('from'));
    let _sqlCount = `select count(*) as total ${sqlPart}`;
    let _sql = `${sql} LIMIT ${limitFirtParam},${limitSecParam}`; 
    var _tmp = [];
    _tmp.push(_getNewSqlParamEntity(_sqlCount,args,PAGE_COUNT));
    _tmp.push(_getNewSqlParamEntity(_sql,args,QUERY))
    return _tmp
}


var service = {
    trans:trans,
    insert:insert,
    query:query,
    update:update,
    remove:remove,
    queryListPage:queryListPage   
}

/**
 * 获取MySQL连接池
 * @param {*} options  mysql的配置
 */
function getPool(options){
    var pool  = mysql.createPool(options);
    pool.on('acquire', function (connection) {
        console.log('Connection %d acquired', connection.threadId);
    });
    return pool;   
}



/**
 * 导出插件
 */

let default_options = {      
                    connectionLimit : 10,
                    host: "localhost",
                    user: "root",
                    password : "",
                    database: "test"		
                }

const optsSchema = Joi.object().keys({
     connectionLimit: Joi.number().integer(),
     host:Joi.string().required(),
     user:Joi.string().required(),
     password:Joi.string().allow('').required(),
     database:Joi.string().required()
 })

exports.register = function (server, opts, next) {
    const result = Joi.validate(opts, optsSchema);
    if(result.error){
        console.error(result.error);
        return;
    }
    const options = Hoek.applyToDefaults(default_options, opts);
    pool = getPool(options);
    server.expose('service', service);
    server.log('info','completed to light-api-mysql plugin init');
    return  next();
};

exports.register.attributes = {
    pkg: require('../package.json')
};
