# JoinDiscovery
Given N relations, discover all relations T1 and T2 when joined under a mapping M gives a join support greater than some threshold T. 

The support is defined as the number of rows from T1 which can join with at least one row from T2 under mapping M.  Mapping is either a attribute or combination of attributes using logical operators(mostly AND).

Usually T1 and T2 are large so computing this straightforward is not an option, so we are exploring how we can do this using Sampling ,more precisely can we look at only say 10% data and draw conclusions about whole data? if so with how much confidence should you be able to say that it is a potential join combination.

Our initial idea is to do this in three steps. 

Step 0 : Reading data sets and dynamically figuring out the schema.

Step 1 : Eliminate obvious combinations using schema incompatibility.  [DONE]

Step 2: Collect statistics about data and its distribution and do a smart pruning. more thought should be put here to avoid true negativities and false positives.

Step 3: Perform several experiments varying both sampling percentage as well as threshold and experimentally find a pair for which it works the best. This procedure can be repeated several times in order to improve the confidence. (On top of postgres)


Sample output for TPCH tables customers and orders

Connected to PgSQL!
Source Attributes List
c_custkey  c_name  c_address  c_nationkey  c_phone  c_acctbal  c_mktsegment  c_comment  

Target Attributes List
o_orderkey  o_custkey  o_orderstatus  o_totalprice  o_orderdate  o_orderpriority  o_clerk  o_shippriority  o_comment  

Datatype compatibility matrix
T T F F F F F T F 
F F T F F T T F T 
F F T F F T T F T 
T T F F F F F T F 
F F T F F T T F T 
F F F T F F F F F 
F F T F F T T F T 
F F T F F T T F T 

Pruned Out Percentage = 62.5


c_custkey vs o_orderkey
c_nationkey vs o_orderkey
c_custkey vs o_custkey
c_nationkey vs o_custkey
c_name vs o_orderstatus
c_address vs o_orderstatus
c_phone vs o_orderstatus
c_mktsegment vs o_orderstatus
c_comment vs o_orderstatus
c_acctbal vs o_totalprice
c_name vs o_orderpriority
c_address vs o_orderpriority
c_phone vs o_orderpriority
c_mktsegment vs o_orderpriority
c_comment vs o_orderpriority
c_name vs o_clerk
c_address vs o_clerk
c_phone vs o_clerk
c_mktsegment vs o_clerk
c_comment vs o_clerk
c_custkey vs o_shippriority
c_nationkey vs o_shippriority
c_name vs o_comment
c_address vs o_comment
c_phone vs o_comment
c_mktsegment vs o_comment
c_comment vs o_comment
(c_custkey,o_custkey) =>92820.0
(c_nationkey,o_custkey) =>90730.0
(c_custkey,o_orderkey) =>36825.0
(c_nationkey,o_orderkey) =>29450.0
(c_nationkey,o_shippriority) =>5700.0
(c_acctbal,o_totalprice) =>3280.0
(c_comment,o_comment) =>3020.0
(c_name,o_orderstatus) =>0.0
(c_address,o_orderstatus) =>0.0
(c_phone,o_orderstatus) =>0.0
(c_mktsegment,o_orderstatus) =>0.0
(c_comment,o_orderstatus) =>0.0
(c_name,o_orderpriority) =>0.0
(c_address,o_orderpriority) =>0.0
(c_phone,o_orderpriority) =>0.0
(c_mktsegment,o_orderpriority) =>0.0
(c_comment,o_orderpriority) =>0.0
(c_name,o_clerk) =>0.0
(c_address,o_clerk) =>0.0
(c_phone,o_clerk) =>0.0
(c_mktsegment,o_clerk) =>0.0
(c_comment,o_clerk) =>0.0
(c_custkey,o_shippriority) =>0.0
(c_name,o_comment) =>0.0
(c_address,o_comment) =>0.0
(c_phone,o_comment) =>0.0
(c_mktsegment,o_comment) =>0.0

<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<
Using Target Scaling with Both Sampling + bloom filter
pairs which satisfy support threshold : 15000
(c_custkey,o_custkey) =>92820.0
(c_nationkey,o_custkey) =>90730.0
(c_custkey,o_orderkey) =>36825.0
(c_nationkey,o_orderkey) =>29450.0
>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>


