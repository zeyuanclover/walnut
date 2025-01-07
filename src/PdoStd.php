<?php

namespace Plantation\Banana\Database;

class PdoStd
{
    /**
     * @var
     * 数据库连接
     */
    protected $dbh;

    /**
     * @var
     * 表前缀
     */
    protected $prefix;

    /**
     * @var
     * 条件
     */
    protected $where =[];

    /**
     * @var
     * 分页
     */
    protected $limit;

    /**
     * @var
     * 分组
     */
    protected $groupBy;

    /**
     * @var
     * 排序
     */
    protected $orderBy;

    /**
     * @var
     * 连接表
     */
    protected $joinTable;

    /**
     * @var
     * 连接条件
     */
    protected $joinWhere;

    /**
     * @var
     * 连接字段
     */
    protected $joinField;

    /**
     * @var
     * 连接类型
     */
    protected $joinType;

    /**
     * @var
     * 开始事务开关
     */
    protected $beginTransaction;

    /**
     * @var
     * havaing 语句
     */
    protected $having;

    /**
     * @var
     * 当前连接名称
     */
    protected $currentDbhName;

    /**
     * @var
     * 不同值开关
     */
    protected $distinct;

    /**
     * @var array
     * or where
     */
    protected $orWhere=[];

    /**
     * @var
     * 排重字段
     */
    protected $distinctStr=[];

    /**
     * @var
     * 是否复制
     */
    protected $copy;

    /**
     * @var
     * 锁表
     */
    protected $lock;

    /**
     * @var
     * debug
     */
    protected $debug;

    /**
     * @param $config
     * @param $logger
     * 构造函数
     */
    public function __construct($config,$logger=false,$name='default'){
        $this->connect($config,$logger,$name);
        return $this;
    }

    /**
     * @param $config
     * @param $logger
     * @return void
     * 连接
     */
    public function connect($config,$logger,$name='default'){
        $dbms = $config['dbms'];
        $host = $config['host'];
        $dbName = $config['db'];    //使用的数据库
        $user = $config['username'];      //数据库连接用户名
        $pass = $config['password'];          //对应的密码
        $dsn="$dbms:host=$host;dbname=$dbName;charset=".$config['charset'];
        $this->prefix[$name] = $config['prefix'];
        if(!$logger){
            $this->dbh[$name] =  new \PDO($dsn, $user, $pass); //初始化一个PDO对象
        }else{
            //默认这个不是长连接，如果需要数据库长连接，需要最后加一个参数：array(PDO::ATTR_PERSISTENT => true) 变成这样：
            $this->dbh[$name] =  new \PDO($dsn, $user, $pass, array(PDO::ATTR_PERSISTENT => true));
        }
        return $this;
    }

    /**
     * @param $config
     * @param $logger
     * @param $name
     * @return void
     * 新增连接
     */
    public function addConnection($config,$logger,$name)
    {
        $this->connect($config,$logger,$name);
    }

    /**
     * @param $name
     * @return $this|void
     * 设置数据库
     */
    public function database($name='default')
    {
        if(isset($this->dbh[$name])){
            $this->currentDbhName = $name;
            return $this;
        }
    }

    /**
     * @return void
     * 关闭连接
     */
    public function disconnect($name='default'){
        $this->dbh[$name]->closeConnection();
        $this->dbh[$name] = null;
    }

    /**
     * @param $prefix
     * @return void
     * 设置表前缀
     */
    public function setPrefix($prefix,$name='default')
    {
        if ($name){
            if(isset($this->dbh[$name])){
                $this->prefix[$name] = $prefix;
            }
        }
        return $this;
    }

    /**
     * @return mixed
     * 获得最后插入id
     */
    public function getLastInsertId()
    {
        return $this->dbh[$this->currentDbhName]->lastInsertId()*1;
    }

    /**
     * @param $table
     * @param $data
     * @return false
     * 插入语句
     */
    public function insert($table, $data,$createSwith=true){
        $vals = [];

        if(!isset($data['CreateAt']) && $createSwith){
            $data['CreateAt'] = time();
        }

        foreach($data as $key => $value) {
            $vals[] = ':'.$key;
        }

        $keys = implode(',', array_keys($data));
        $value = implode(',', $vals);
        //echo "INSERT INTO ".$this->prefix[$this->currentDbhName]."$table ($keys) VALUES ($value)";exit;
        $stmt = $this->dbh[$this->currentDbhName]->prepare("INSERT INTO ".$this->prefix[$this->currentDbhName]."$table ($keys) VALUES ($value)");

        $value = null;
        $keys = null;

        foreach ($data as $key=>$val){
            $stmt->bindValue(':'.$key, $val);
        }

        if($stmt->execute()){
            return $this->dbh[$this->currentDbhName]->lastInsertId();
        }else{
            return false;
        }
    }

    /**
     * @param $table
     * @param $data
     * @return int[]
     * 一次插入多条数据
     */
    public function insertMulti($table, $data){
        $data = ['success'=>0,'errot'=>0];
        foreach ($data as $val){
            $rs = $this->insert($table, $val);
            if ($rs>0){
                $data['success'] += 1;
            }else{
                $data['errot'] += 1;
            }
        }
        return $data;
    }

    /**
     * @param $table
     * @param $data
     * @return mixed
     * 更新语句
     */
    public function update($table, $data)
    {
        $sql = "UPDATE ".$this->prefix[$this->currentDbhName]."$table SET ";
        if(!isset($data['UpdateAt'])){
            $data['UpdateAt'] = time();
        }
        $rs = $this->execQuery($data,$sql,true);
        return $rs->rowCount();
    }

    /**
     * @param $where
     * @return string
     * where 拼接
     */
    public function whereQueryExec($where,$sy='AND')
    {
        // where
        $whereVals = [];
        foreach($where as $key => $value) {
            foreach ($value as $key2 => $value2) {
                foreach ($value2 as $key3 => $value3) {
                    $keyCondition = trim(strtolower($key3));
                    $keyb = $key2;

                    if($keyCondition == 'like'){
                        $key2 = ' '.$keyCondition;
                        $whereVals[] = ''.$keyb.' LIKE'." ?";
                    }else{
                        if($keyCondition == 'between'){
                            $key2 = ' '.$keyCondition;
                            $whereVals[] = $keyb.' BETWEEN ? AND ?';
                        }elseif($keyCondition=='is'){
                            $whereVals[] = $keyb.' IS ?';
                        }elseif($keyCondition=='is not'){
                            $whereVals[] = $keyb.' IS NOT ?';
                        } elseif($keyCondition == 'in'){
                            $s = [];
                            foreach ($value3 as $key5 => $value3) {
                                $s[]= '?';
                            }
                            $whereVals[] = $keyb." IN (".implode(',',$s).")";
                        }elseif($keyCondition == 'not in'){
                            $s = [];
                            foreach ($value3 as $key5 => $value3) {
                                $s[]= '?';
                            }
                            $whereVals[] = $keyb." NOT IN (".implode(',',$s).")";
                        }elseif($keyCondition == 'not like'){
                            $key2 = ' '.$keyCondition;
                            $whereVals[] = ''.$keyb.' NOT LIKE'." ?";
                        }elseif($keyCondition == 'not between'){
                            $key2 = ' '.$keyCondition;
                            $whereVals[] = $keyb.' NOT BETWEEN ? AND ?';
                        }
                        else{
                            $whereVals[] = ''.$keyb.$keyCondition.'?';
                        }
                    }
                }
            }
        }
        //print_r($whereVals);exit;
        $whereValsalue = '';
        if(count($whereVals) > 0){
            $whereValsalue = implode(' '.$sy.' ', $whereVals);
        }
        return $whereValsalue;
    }

    /**
     * @param $data
     * @param $sql
     * @param $isResult
     * @param $limit
     * @return bool
     * 执行语句
     */
    public function execQuery($data=[],$sql='',$isResult=false,$limit=0){
        if(isset($this->where[$this->currentDbhName]) && $this->where[$this->currentDbhName]){
            $where = $this->where[$this->currentDbhName];
        }else{
            $where = [];
        }

        $keys = implode(',', array_keys($data));
        $vals = [];
        foreach($data as $key => $value) {
            $vals[] = ''.$key.'=?';
        }

        $whereValsalue = $this->whereQueryExec($where,'AND');
        $orWhereValsalue = null;
        if(isset($this->orWhere[$this->currentDbhName]) && $this->orWhere[$this->currentDbhName]){
            $orWhereValsalue = $this->whereQueryExec($this->orWhere[$this->currentDbhName],'OR');
        }

        if(!$whereValsalue){
            $whereValsalue = 1;
        }

        if($whereValsalue){
            $whereValsalue = ' WHERE '.$whereValsalue;
            if($orWhereValsalue){
                $orWhereValsalue = ' OR '.$orWhereValsalue;
            }
        }else{
            $orWhereValsalue =' WHERE '.$orWhereValsalue;
        }

        $updateValue = implode(',', $vals);

        if(!isset($this->limit[$this->currentDbhName])){
            $this->limit[$this->currentDbhName] = null;
        }

        if ($limit){
            $limit = " LIMIT $limit";
        }else{
            if($this->limit[$this->currentDbhName]){
                $limit = " LIMIT ".$this->limit[$this->currentDbhName];
            }else{
                $limit = "";
            }
        }

        $groupBy = '';
        if(isset($this->groupBy[$this->currentDbhName]) && $this->groupBy[$this->currentDbhName]){
            $groupBy = " GROUP BY ".$this->groupBy[$this->currentDbhName];
        }else{
            $groupBy = '';
        }

        $orderBy = '';
        if(isset($this->orderBy[$this->currentDbhName]) && $this->orderBy[$this->currentDbhName]){
            $orderBy = $this->orderBy[$this->currentDbhName];
        }else{
            $orderBy = '';
        }

        $joinSql = '';
        if(isset($this->joinTable[$this->currentDbhName]) && is_array($this->joinTable[$this->currentDbhName])){
            foreach($this->joinTable[$this->currentDbhName] as $key=>$join){
                $joinSql .= strtoupper($this->joinType[$this->currentDbhName][$key]).' 
                JOIN '.$this->prefix[$this->currentDbhName].$join.' ON '.
                    $this->joinWhere[$this->currentDbhName][$key];
            }
        }

        if(!isset($this->having[$this->currentDbhName])){
            $this->having[$this->currentDbhName] = '';
        }

        //echo "$sql $updateValue $joinSql $whereValsalue $orWhereValsalue $groupBy $orderBy ".$this->having[$this->currentDbhName]." $limit";//exit;
        $stmt = $this->dbh[$this->currentDbhName]->prepare("$sql $updateValue $joinSql $whereValsalue $orWhereValsalue $groupBy $orderBy ".$this->having[$this->currentDbhName]." $limit");

        $value = null;
        $keys = null;

        $bindKeyStep = 1;
        $newData = [];
        foreach ($data as $key=>$val){
            $stmt->bindValue($bindKeyStep, $val,\PDO::PARAM_STR);
            $bindKeyStep+=1;
        }

        $whereArr = [];
        //print_r($whereValsalue);exit;
        if($whereValsalue){
            foreach ($where as $key => $value){
                foreach ($value as $keyb => $value2) {
                    foreach ($value2 as $key3 => $value3) {
                        $key2 = trim(strtolower($key3));
                        if($key2 == 'like'||$key2 == 'not like') {
                            $stmt->bindValue($bindKeyStep, $value3,\PDO::PARAM_STR);
                            $bindKeyStep +=1;
                        }else{
                            if($key2 == 'between'){
                                $stmt->bindValue($bindKeyStep, $value3['begin'],\PDO::PARAM_STR);
                                $stmt->bindValue($bindKeyStep+1, $value3['end'],\PDO::PARAM_STR);
                                $bindKeyStep+=2;
                            }elseif($key2 =='is'){
                                $stmt->bindValue($bindKeyStep, $value3,\PDO::PARAM_STR);
                                $bindKeyStep +=1;
                            }elseif($key2 =='is not'){
                                $stmt->bindValue($bindKeyStep, $value3,\PDO::PARAM_STR);
                                $bindKeyStep +=1;
                            }elseif($key2 == 'not between'){
                                $stmt->bindValue($bindKeyStep, $value3['begin'],\PDO::PARAM_STR);
                                $stmt->bindValue($bindKeyStep+1, $value3['end'],\PDO::PARAM_STR);
                                $bindKeyStep+=2;
                            }
                            elseif($key2=='in'){
                                foreach ($value3 as $keyr => $valuer) {
                                    $stmt->bindValue($bindKeyStep+$keyr, $valuer,\PDO::PARAM_STR);
                                }
                                $bindKeyStep += array_key_last($value3)+1;
                            }elseif($key2=='not in'){
                                foreach ($value3 as $keyd => $valuet) {
                                    $stmt->bindValue($bindKeyStep+$keyd, $valuet,\PDO::PARAM_STR);
                                }
                                $bindKeyStep += array_key_last($value3)+1;
                            }
                            else{
                                //echo $bindKeyStep;
                                $stmt->bindValue($bindKeyStep, $value3,\PDO::PARAM_STR);
                                $bindKeyStep +=1;
                            }
                        }
                    }
                }
            }
        }

        if(isset($this->orWhere[$this->currentDbhName]) && count($this->orWhere[$this->currentDbhName]) > 0){
            foreach ($this->orWhere[$this->currentDbhName] as $key => $value){
                foreach ($value as $keyb => $value2) {
                    foreach ($value2 as $key3 => $value3) {
                        $key2 = trim(strtolower($key3));
                        if($key2 == 'like'||$key2 == 'not like') {
                            $stmt->bindValue($bindKeyStep, $value3,\PDO::PARAM_STR);
                            $bindKeyStep +=1;
                        }else{
                            if($key2 == 'between'){
                                $stmt->bindValue($bindKeyStep, $value3['begin'],\PDO::PARAM_STR);
                                $stmt->bindValue($bindKeyStep+1, $value3['end'],\PDO::PARAM_STR);
                                $bindKeyStep+=2;
                            }elseif($key2 =='is'){
                                $stmt->bindValue($bindKeyStep, $value3,\PDO::PARAM_STR);
                                $bindKeyStep +=1;
                            }elseif($key2 =='is not'){
                                $stmt->bindValue($bindKeyStep, $value3,\PDO::PARAM_STR);
                                $bindKeyStep +=1;
                            }elseif($key2 == 'not between'){
                                $stmt->bindValue($bindKeyStep, $value3['begin'],\PDO::PARAM_STR);
                                $stmt->bindValue($bindKeyStep+1, $value3['end'],\PDO::PARAM_STR);
                                $bindKeyStep+=2;
                            }
                            elseif($key2=='in'){
                                foreach ($value3 as $keyr => $valuer) {
                                    $stmt->bindValue($bindKeyStep+$keyr, $valuer,\PDO::PARAM_STR);
                                }
                                $bindKeyStep += array_key_last($value3)+1;
                            }elseif($key2=='not in'){
                                foreach ($value3 as $keyd => $valuet) {
                                    $stmt->bindValue($bindKeyStep+$keyd, $valuet,\PDO::PARAM_STR);
                                }
                                $bindKeyStep += array_key_last($value3)+1;
                            }
                            else{
                                $stmt->bindValue($bindKeyStep, $value3,\PDO::PARAM_STR);
                                $bindKeyStep +=1;
                            }
                        }
                    }
                }
            }
        }

        // 是否复位
        if(!isset($this->copy[$this->currentDbhName]) || $this->copy[$this->currentDbhName]==false){
            $this->reset();
        }

        // 复位
        $this->copy[$this->currentDbhName] = false;

        if($stmt->execute()){
            if(isset($this->debug[$this->currentDbhName]) && $this->debug[$this->currentDbhName]==true){
                echo $stmt->debugDumpParams();
                exit;
            }
            if($isResult===true){
                $this->debug[$this->currentDbhName] = false;
                return $stmt;
            }else{
                $this->debug[$this->currentDbhName] = false;
                return true;
            }
        }else{
            $this->debug[$this->currentDbhName] = false;
            return false;
        }
    }

    /**
     * @param $table
     * @return bool
     * 删除记录
     */
    public function delete($table){
        $sql = "DELETE FROM ".$this->prefix[$this->currentDbhName]."$table ";
        return $this->execQuery([],$sql);
    }

    /**
     * @return string
     * 获得连接字段
     */
    public function getJoinFields()
    {
        $joinField = [];
        if(isset($this->joinTable[$this->currentDbhName]) && is_array($this->joinTable[$this->currentDbhName])){
            foreach($this->joinField[$this->currentDbhName] as $join){
                $joinField[] = $join;
            }
        }

        return implode(',',$joinField);
    }

    /**
     * @param $table
     * @param $fields
     * @return array
     * 获得多个列
     */
    public function get($table,$num=0,$fields='*'){
        $distinct = null;
        if(isset($this->distinct[$this->currentDbhName])&&$this->distinct[$this->currentDbhName]===true){
            $distinct = 'DISTINCT ' .$this->distinctStr;
        }else{
            $distinct = "$fields";
        }

        $joinField = rtrim($this->getJoinFields(),',');
        if($joinField){
            $joinField = ','.$joinField;
        }

        if($num>0){
            $this->limit("0,$num");
        }

        $sql = "SELECT $distinct $joinField FROM ".$this->prefix[$this->currentDbhName]."$table ";
        $rows = $this->execQuery([],$sql,true);
        $data = [];
        while ($row = $rows->fetch(\PDO::FETCH_ASSOC)) {
            $data[] = $row;
        }
        return $data;
    }

    /**
     * @param $table
     * @param $fields
     * @return mixed
     * 获得一列的值
     */
    public function find($table,$fields='*')
    {
        return $this->getOne($table,$fields);
    }

    /**
     * @param $table
     * @param $fields
     * @return mixed
     * 获得一列的值
     */
    public function getOne($table,$fields='*')
    {
        $distinct = null;
        if(isset($this->distinct[$this->currentDbhName]) && $this->distinct[$this->currentDbhName]===true){
            $distinct = 'DISTINCT ' .$this->distinctStr;
        }else{
            $distinct = "$fields";
        }

        $joinField = rtrim($this->getJoinFields(),',');
        if($joinField){
            $joinField = ','.$joinField;
        }

        $sql = "SELECT $distinct $joinField FROM ".$this->prefix[$this->currentDbhName]."$table ";
        $rows = $this->execQuery([],$sql,true,1);
        $data = [];
        return $rows->fetch(\PDO::FETCH_ASSOC);
    }

    /**
     * @param $table
     * @param $fields
     * @return null
     * 获得某一列某个值
     */
    public function getValue($table,$fields='*'){
        $distinct = null;
        if(isset($this->distinct[$this->currentDbhName]) && $this->distinct[$this->currentDbhName]===true){
            $distinct = 'DISTINCT' .$this->distinctStr;
        }

        $joinField = rtrim($this->getJoinFields(),',');
        if($joinField){
            $joinField = ','.$joinField;
        }

        $sql = "SELECT $distinct $fields $joinField FROM ".$this->prefix[$this->currentDbhName]."$table ";
        $rows = $this->execQuery([],$sql,true,1);
        $data = [];
        $data = $rows->fetch(\PDO::FETCH_ASSOC);
        $rows = null;

        if($fields='*'){
            return $data;
        }

        if (isset($data[$fields])) {
            return $data[$fields];
        }else{
            return null;
        }
    }

    /**
     * @param $table
     * @param $field
     * @param $value
     * @return mixed
     * 自减
     */
    public function dec($table,$field,$value=1){
        $sql = "UPDATE ".$this->prefix[$this->currentDbhName]."$table SET `$field`=$field-$value ";
        $rs = $this->execQuery([],$sql,true);
        return $rs->rowCount();
    }

    /**
     * @param $table
     * @param $field
     * @param $value
     * @return mixed
     * 自增
     */
    public function inc($table,$field,$value=1){
        $sql = "UPDATE ".$this->prefix[$this->currentDbhName]."$table SET `$field`=$field+$value ";
        $rs = $this->execQuery([],$sql,true);
        return $rs->rowCount();
    }

    /**
     * @param $limit
     * @param $pager
     * @param $fields
     * @return array
     * 分页
     */
    public function page($table,$pager=1,$limit=10,$fields='*'){
        $pager-=1;
        $limitBegin = $limit * $pager;
        return $this->limit("$limitBegin,$limit")->get($table,$fields);
    }

    /**
     * @param $table
     * @param $fields
     * @return mixed
     * 统计总数
     */
    public function count($table,$fields='*'){
        $f = "count($fields)";

        $joinField = $this->getJoinFields();
        if($joinField){
            $joinField = ','.$joinField;
        }

        $sql = "SELECT $f $joinField FROM ".$this->prefix[$this->currentDbhName]."$table ";
        $rows = $this->execQuery([],$sql,true,1);
        $data = [];
        $data = $rows->fetch(\PDO::FETCH_ASSOC);
        $rows = null;
        return $data;
    }

    /**
     * @param $limit
     * @return $this
     * 分页
     */
    public function limit($limit)
    {
        $this->limit[$this->currentDbhName] = $limit;
        return $this;
    }

    /**
     * @param $field
     * @return $this
     * 分组
     */
    public function groupBy($field)
    {
        $this->groupBy[$this->currentDbhName] = $field;
        return $this;
    }

    /**
     * @param $field
     * @param $order
     * @return $this
     * 排序
     */
    public function orderBy($field,$order='ASC')
    {
        $this->orderBy[$this->currentDbhName] = " ORDER BY $field $order";
        return $this;
    }

    /**
     * @param $field
     * @return void
     * having
     */
    public function having($field)
    {
        $this->having[$this->currentDbhName] = $field;
        return $this;
    }

    /**
     * @param $state
     * @return void
     * 获得不重复的值
     */
    public function distinct($fields)
    {
        $this->distinctStr[$this->currentDbhName] = '';
        $this->distinctStr[$this->currentDbhName] = rtrim($fields,',');
        $this->distinct[$this->currentDbhName] = true;
        return $this;
    }

    /**
     * @param $name
     * @param $value
     * @param $condition
     * @return $this
     * or where
     */
    public function orWhere($name,$value=null,$condition='=')
    {
        if(is_array($name)){
            if(!isset($name[0])){
                $name = [$name];
            }
            foreach ($name as $k=>$v){
                $this->orWhere[$this->currentDbhName][] = $v;
            }
            return $this;
        }

        $condition = trim(strtolower($condition));
        $orWhere = [];
        if($condition=='='){
            $orWhere[$name]['='] = $value;
        }

        if(in_array($condition,['>','>=','<','<=','<>'])){
            $orWhere[$name] = [$condition=>$value];
        }

        if($condition=='like'){
            $orWhere[$name] = ['LIKE'=>$value];
        }

        if($condition=='is'){
            $orWhere[$name] = ['IS'=>$value];
        }

        if($condition=='is not'){
            $orWhere[$name] = ['IS NOT'=>$value];
        }

        if($condition=='not like'){
            $orWhere[$name] = ['NOT LIKE'=>$value];
        }

        if($condition=='between'){
            $orWhere[$name] = ['BETWEEN'=>['begin'=>$value['0'],'end'=>$value['1']]];
        }

        if($condition=='not between'){
            $orWhere[$name] = ['NOT BETWEEN'=>['begin'=>$value['0'],'end'=>$value['1']]];
        }

        if($condition=='in'){
            $orWhere[$name] = ['IN'=>$value];
        }

        if($condition=='not in'){
            $orWhere[$name] = ['NOT IN'=>$value];
        }

        $this->orWhere[$this->currentDbhName][] = $orWhere;
        return $this;
    }

    /**
     * @param $name
     * @param $value
     * @param $condition
     * @return $this
     * where 条件
     */
    public function where($name,$value=null,$condition='='){
        if(is_array($name)){
            if(!isset($name[0])){
                $name = [$name];
            }
            foreach ($name as $k=>$v){
                $this->where[$this->currentDbhName][] = $v;
            }
            return $this;
        }

        $condition = trim(strtolower($condition));
        $where = [];
        if($condition=='='){
            $where[$name]['='] = $value;
        }

        if(in_array($condition,['>','>=','<','<=','<>'])){
            $where[$name] = [$condition=>$value];
        }

        if($condition=='is'){
            $where[$name] = ['IS'=>$value];
        }

        if($condition=='is not'){
            $where[$name] = ['IS NOT'=>$value];
        }

        if($condition=='like'){
            $where[$name] = ['LIKE'=>$value];
        }

        if($condition=='not like'){
            $where[$name] = ['NOT LIKE'=>$value];
        }

        if($condition=='between'){
            $where[$name] = ['BETWEEN'=>['begin'=>$value['0'],'end'=>$value['1']]];
        }

        if($condition=='not between'){
            $where[$name] = ['NOT BETWEEN'=>['begin'=>$value['0'],'end'=>$value['1']]];
        }

        if($condition=='in'){
            $where[$name] = ['IN'=>$value];
        }

        if($condition=='not in'){
            $where[$name] = ['NOT IN'=>$value];
        }

        $this->where[$this->currentDbhName][] = $where;
        return $this;
    }

    /**
     * @return void
     * 开始事务
     */
    public function beginTransaction()
    {
        $this->dbh[$this->currentDbhName]->beginTransaction();
        return $this;
    }

    /**
     * @return void
     * 提交事务
     */
    public function commit()
    {
        $this->dbh[$this->currentDbhName]->commit();
        return $this;
    }

    /**
     * @return void
     * 回滚事务
     */
    public function rollback()
    {
        $this->dbh[$this->currentDbhName]->rollback();
        return $this;
    }

    /**
     * @param $table
     * @param $type
     * @param $onWhere
     * @param $joinFields
     * @return $this
     * join 连接表
     */
    public function join($table,$onWhere,$type='left',$joinFields='a.*'){
        $this->joinTable[$this->currentDbhName][] = $table;
        $this->joinField[$this->currentDbhName][] = $joinFields;
        $this->joinWhere[$this->currentDbhName][] = $onWhere;
        $this->joinType[$this->currentDbhName][] = strtoupper($type);
        return $this;
    }

    /**
     * @param $sql
     * @param $value
     * @return mixed
     * 执行复杂语句
     */
    public function exec($sql,$value=[])
    {
        $stmt = $this->dbh[$this->currentDbhName]->prepare("$sql");
        $stmt->execute($value);
        return $stmt;
    }

    /**
     * @return void
     * 复位
     */
    public function reset(){
        $this->joinTable[$this->currentDbhName] = [];
        $this->joinField[$this->currentDbhName] = [];
        $this->joinWhere[$this->currentDbhName] = [];
        $this->joinType[$this->currentDbhName] = [];
        $this->where[$this->currentDbhName] = [];
        $this->orWhere[$this->currentDbhName] = [];
        $this->limit[$this->currentDbhName] = null;
        $this->groupBy[$this->currentDbhName] = '';
        $this->orderBy[$this->currentDbhName] = '';
        $this->beginTransaction[$this->currentDbhName] = false;
        $this->having[$this->currentDbhName] = '';
        $this->distinct[$this->currentDbhName] = false;
        $this->lock[$this->currentDbhName] = '';
        $this->distinctStr = '';
    }

    /**
     * @param $table
     * @return void
     * 锁表
     */
    public function lock($table)
    {
        $this->lock[$this->currentDbhName] = $table;
        $this->exec("LOCK TABLES `$table` WRITE");
        return $this;
    }

    /**
     * @return void
     * 解除锁定
     */
    public function unlock(){
        $this->lock[$this->currentDbhName] = '';
        $this->exec("UNLOCK TABLES");
        return $this;
    }

    /**
     * @return void
     * 复制属性
     */
    public function copy()
    {
        $this->copy[$this->currentDbhName] = true;
        return $this;
    }

    /**
     * @return void
     * 清除搜索条件
     */
    public function clearCopy()
    {
        $this->copy[$this->currentDbhName] = false;
        return $this;
    }

    /**
     * @return void
     * debug
     */
    public function debug()
    {
        $this->debug[$this->currentDbhName] = true;
        return $this;
    }

    /***
     * @param $table
     * @param $deleted
     * @return mixed
     * 软删除
     */
    public function softDelete($table,$deleted=1)
    {
        $sql = "UPDATE ".$this->prefix[$this->currentDbhName]."$table SET `IsDeleted`=$deleted ";
        $rs = $this->execQuery([],$sql,true);
        return $rs->rowCount();
    }
}