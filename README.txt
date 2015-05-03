//Go through the "Presentation" folder to learn more about this project.
//To see results of your experiments refer to "Results" folder. 

JoinDiscovery
-------------
Given N relations, discover all relations T1 and T2 when joined under a mapping M gives a join support greater than some threshold T. 

The support is defined as the number of rows from T1 which can join with at least one row from T2 under mapping M.  Mapping is either a attribute or combination of attributes using logical operators(mostly AND).

Usually T1 and T2 are large so computing this straightforward is not an option, so we are exploring how we can do this using Sampling ,more precisely can we look at only say 10% data and draw conclusions about whole data? if so with how much confidence should you be able to say that it is a potential join combination.

Our initial idea is to do this in three steps. 

Step 0 : Reading data sets and dynamically figuring out the schema.

Step 1 : Eliminate obvious combinations using schema incompatibility.  

Step 2: Collect statistics about data and its distribution and do a smart pruning. more thought should be put here to avoid true negativities and false positives.

Step 3: Perform several experiments varying both sampling percentage as well as threshold and experimentally find a pair for which it works the best. This procedure can be repeated several times in order to improve the confidence. (On top of postgres)


Here's the sample output for supplier vs partsupp for TPCH 1Gb data.
-------------------------------------------------------------------
For this s= 0.1 for both source and destination and t = 0.1.
(See code to learn more about these attributes, code is commented moderately)


Source Attributes List
s_suppkey  s_name  s_address  s_nationkey  s_phone  s_acctbal  s_comment  

Target Attributes List
ps_partkey  ps_suppkey  ps_availqty  ps_supplycost  ps_comment  
Datatype compatibility matrix
T T T F F 
F F F F T 
F F F F T 
T T T F F 
F F F F T 
F F F T F 
F F F F T 

Pruned Out Percentage = 68.57142857142857
Candidates
[3] , [0] ,Support = 100% {Sanity bound}
[0] , [0] ,Support = 100% {Sanity bound}
[0] , [1] ,Support = 100% {Sanity bound}
[0] , [2] ,Support = 99.36%
[3] , [1] ,Support = 96.2%
[3] , [2] ,Support = 95.58%
[5] , [3] ,Support = 10.65%
[1] , [4] ,Support = 0%
[2] , [4] ,Support = 0%
[4] , [4] ,Support = 0%
[6] , [4] ,Support = 0%

Frequents
[3] , [0] ,Support = 100% {Sanity bound}
[0] , [0] ,Support = 100% {Sanity bound}
[0] , [1] ,Support = 100% {Sanity bound}
[0] , [2] ,Support = 99.36%
[3] , [1] ,Support = 96.2%
[3] , [2] ,Support = 95.58%
[5] , [3] ,Support = 10.65%

Candidates
[0, 3] , [1, 2] ,Support = 1.98%
[0, 3] , [2, 1] ,Support = 1%
[3, 0] , [0, 1] ,Support = 0%
[3, 0] , [0, 2] ,Support = 0%
[3, 5] , [0, 3] ,Support = 0%
[0, 3] , [0, 1] ,Support = 0%
[0, 3] , [0, 2] ,Support = 0%
[0, 5] , [0, 3] ,Support = 0%
[0, 5] , [1, 3] ,Support = 0%
[0, 5] , [2, 3] ,Support = 0%
[3, 5] , [1, 3] ,Support = 0%
[3, 5] , [2, 3] ,Support = 0%

Frequents
 Empty


Final Join Pairs: count = 7
[3] , [0] ,Support = 100% {Sanity bound}
[0] , [0] ,Support = 100% {Sanity bound}
[0] , [1] ,Support = 100% {Sanity bound}
[0] , [2] ,Support = 99.36%
[3] , [1] ,Support = 96.2%
[3] , [2] ,Support = 95.58%
[5] , [3] ,Support = 10.65%


The actual join pairs are
Final Join Pairs: count = 6
[0] , [0] ,Support = 100%
[0] , [1] ,Support = 100%
[0] , [2] ,Support = 99.99%
[3] , [0] ,Support = 95.8%
[3] , [1] ,Support = 95.8%
[3] , [2] ,Support = 95.8%

