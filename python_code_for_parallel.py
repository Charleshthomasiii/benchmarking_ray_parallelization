import ray
import time
import random
def timerfunc(func):
    """
    A timer decorator
    """
    def function_timer(*args, **kwargs):
        """
        A nested function for timing other functions
        """
        start = time.time()
        value = func(*args, **kwargs)
        end = time.time()
        runtime = end - start
        msg = "The runtime for {func} took {time} seconds to complete"
        print(msg.format(func=func.__name__,
                         time=runtime))
        return value
    return function_timer

ray.init()

@ray.remote
class CoreUnit():
    def __init__(self, values):
        # self.left_bound  = left_bound #inclusive
        # self.right_bound = right_bound #exclusive
        self.values=values
        self.sum = 0

    def generate_sum(self):
        for i in self.values:
            self.sum += i
    def return_sum(self):
        return self.sum

@timerfunc
def setup_cores(integers, cores):
    partition_length = len(integers)//cores
    remainder = len(integers)%cores
    counters = []
    for core_num in range(cores):
        partition=integers[core_num*partition_length:core_num*partition_length+partition_length]
        # print(partition)
       # for offset in range(partition_length):
        # partition.append(integers[core_num*partition_length + offset])
        if remainder != 0 and core_num==cores-1:
            for x in range(remainder):
                partition.append(integers[(cores)*partition_length+x])
        counters.append(CoreUnit.remote(partition))
    return counters

@timerfunc
def compute_sum(counters):
    for c in counters:
        c.generate_sum.remote()
    results = [c.return_sum.remote() for c in counters]     
    print(ray.get(results))


array =[random.randint(0,100)]*10000000
for x in range(4):
    print('----------------calculating sum for {} cores----------------'.format(x+1))
    counters = setup_cores(array,x+1)
    compute_sum(counters)
    # counters = [CoreUnit.remote([1,2,3,4]) for i in range(cores)]
# counters = [CoreUnit.remote([1,2,3,4]) for i in range(4)]
