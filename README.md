Project Description
In this project, you are asked to implement a simple graph algorithm that needs two Map-Reduce jobs. A directed graph is represented as a text file where each line represents a graph edge. For example,

20,40
represents the directed edge from node 20 to node 40. First, for each graph node, you compute the number of node neighbors. Then, you group the nodes by their number of neighbors and for each group you count how many nodes belong to this group. That is, the result will have lines such as:
10 30
which says that there are 30 nodes that have 10 neighbors. To help you, I am giving you the pseudo code. The first Map-Reduce is:
map ( key, line ):
  read 2 long integers from the line into the variables key2 and value2
  emit (key2,value2)

reduce ( key, nodes ):
  count = 0
  for n in nodes
      count++
  emit(key,count)
The second Map-Reduce is:
map ( node, count ):
  emit(count,1)

reduce ( key, values ):
  sum = 0
  for v in values
      sum += v
  emit(key,sum)
