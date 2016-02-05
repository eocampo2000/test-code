'''
Created on Feb 8, 2013

@author: eocampo

This class implements a Priority queue
'''
__version__ = '20130208'
import heapq
 
class PriorityQueue:
 
    # Helper class to manage the cost comparisons
    class Node:
        def __init__(self, data, cost):
            self.data = data
            self.cost = cost
 
        def __cmp__(self,rhs):
            return cmp(self.cost,rhs.cost)
        
    def __init__(self,defcost):
        self.heap = []
        self.defcost = defcost  
        
 
    def insert(self, data, cost):
        n = PriorityQueue.Node(data,cost)
        heapq.heappush(self.heap,n)
        
 
    def top(self):
        assert(not self.isEmpty())
        return self.heap[0].data
 
    def isEmpty(self):
        return self.heap == []
 
    def size(self):
        return len(self.heap)
 
    def priority(self):
        cost = self.defcost
        if not self.isEmpty():
            cost = self.heap[0].cost
        return cost
 
    def pop(self):
        assert(not self.isEmpty())
        return heapq.heappop(self.heap).data
 
    def remove(self,data):
        i = -1
        for j in range(len(self.heap)):
            if self.heap[j].data == data:
                i = j
                break
        if i == -1:
            return
        self.heap = self.heap[:i]+self.heap[i+1:]
        heapq.heapify(self.heap)
# 
 
class Stack(object): 
    def __init__(self): 
        self.l = [] 
        self.n = 0
          
    def push(self, element): 
        if len(self.l) > self.n: 
            self.l[self.n] = element 
        else: 
            self.l.append(element) 
        self.n += 1
          
    def pop(self): 
        if self.n == 0: return None
        self.n -= 1
        return self.l[self.n] 
      
    def size(self): 
        return self.n 
      
    def elements(self): 
        return [self.l[i] for i in xrange(0, self.n)] 

    def bk_elements(self): 
        for i in reversed(self.l):
            print i


# Link List
class SListNode(object):
    
    "Singly-linked list node"
    def __init__(self,next_node = None,my_data = None):
        self.next = next_node
        self.data = my_data

    def GetNextNode(self):
        return self.next

    def SetNextNode(self,next_node):
        self.next = next_node

    def GetData(self):
        return self.data

    def SetData(self,my_data):
        self.data = my_data


class SList(object):
    "Singly-linked list"
    def __init__(self):
        self.root = SListNode()
        self.root.SetNextNode(self.root)
        self.length = 0;

    def GetRootNode(self):
        return self.root

    def Insert(self,data):
        new_node = SListNode()
        new_node.SetData(data)
        new_node.SetNextNode(self.root.GetNextNode())
        self.root.SetNextNode(new_node)
        self.length = self.length + 1

    def GetLength(self):
        return self.length

    def Traverse(self):
        tnode = self.root.GetNextNode()
        while tnode != self.root:
            print str(tnode.GetData()) + ' ',
            tnode = tnode.GetNextNode()


def test_ll():
    intlist = SList()
    for x in range(10):
         intlist.Insert(x + 1)
    print 'There are %d elements in the list' % intlist.GetLength()
    intlist.Traverse()



def test_queue():
    q = PriorityQueue(defcost = 100)
    q.insert( "three", 3 )
    q.insert( "one.1", 1 )
    q.insert( "one.3", 1 )
    q.insert( "two", 2 )
    q.insert( "one.2", 1 )
    q.remove("two")
    print ("SIZE = %d" % q.size())


    sz = q.size()
    for n in range(sz):
        print '---'
        print "PRIORITY = " , q.priority()
        print "TOP    = " , q.top()
        print "POP    ="  ,q.pop()


def test_stack():
    s = Stack()
    s.push(['wk2.5',0])
    s.push(['wk2.4',0])
    s.push(['wk2.3',0])
    s.push(['wk2.2',0])
    s.push(['wk2.1',0])
    s.push(['pipe2',0])
    s.push(['wk1.3',0])
    s.push(['wk1.2',0])
    s.push(['wk1.1',0])
    s.push(['pipe1',0])
    s.push(['Group1',0])

    print "size %d " % s.size()
    print "Elements " , s.elements()
    print "BK Elements " , s.bk_elements()
if __name__ == "__main__":
    test_queue()
    #test_stack()
    #test_ll()
