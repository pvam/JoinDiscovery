# JoinDiscovery
Given N relations, discover all relations T1 and T2 when joined under a mapping M gives a join support greater than some threshold T. 

The support is defined as the number of rows from T1 which can join with at least one row from T2 under mapping M.  Mapping is either a attribute or combination of attributes using logical operators(mostly AND).

Usually T1 and T2 are large so computing this straightforward is not an option, so we are exploring how we can do this using Sampling ,more precisely can we look at only say 10% data and draw conclusions about whole data? if so with how much confidence should you be able to say that it is a potential join combination.

Our initial idea is to do this in three steps. 

Step 0 : Reading data sets and dynamically figuring out the schema.

Step 1 : Eliminate obvious combinations using schema incompatibility.  [DONE]

Step 2: Collect statistics about data and its distribution and do a smart pruning. more thought should be put here to avoid true negativities and false positives.

Step 3: Perform several experiments varying both sampling percentage as well as threshold and experimentally find a pair for which it works the best. This procedure can be repeated several times in order to improve the confidence. (On top of postgres)



