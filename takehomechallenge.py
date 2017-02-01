from random import randint

#===============================================================================

def estimateDistinctElements(items, k):
	'''
	This function will return an estimate to the number of distinct elements in items.

	NOTE - The hash function generates floating point numbers between 0 and 1 
	using the formula:

	h(x) = ((ax + b) % c) / c
	a = random int less than the max value of x
	b = random int less than the max value of x
	x = the integer hash value of the element
	c = the next prime number larger than the max value of x

	items - a sequence of elements
	k - number of hash functions
	'''
	approx = 0
	max_val = 2 ** 32 - 1
	nextPrime = 4294967311

	for _ in xrange(k):
		a = randint(1, max_val-1)
		b = randint(1, max_val-1)
		approx += min(((a*hash(elem)+b)%nextPrime)/float(nextPrime) for elem in items)

	approx   = approx / k
	estimate = round(1 / approx - 1, 3)
	return approx

def estimateDistinctElementsParallel(listsOfItems, k):
	'''
	This function will return an estimate to the number of distinct elements in items (same as
	above) with the distinction that listsOfItems is now a list of sequences. The idea behind this
	function is to generate partial estimates on every sequence and then combine them into a
	single estimate. 

	Note: this function should simulate a distributed environment, so
	assume the list of sequences is just an abstraction, and that every individual sequence
	is on a different physical machine and that the combined size of all those lists cannot
	fit on any single machine.

	listOfItems - a sequence of elements
	k - number of hash functions
	'''
	# generates indexable list of (a, b) for each hash function
	max_val = 2 ** 32 - 1
	k_values = [(randint(1, max_val-1), randint(1, max_val-1)) for _ in xrange(k)]

	def mapper(seq, k, k_vals):
		'''
		Map list of items to vector of k minimums
		'''
		nextPrime = 4294967311

		for i in xrange(k):
			a = k_vals[i][0]
			b = k_vals[i][1]
			yield min(((a*hash(elem)+b)%nextPrime)/float(nextPrime) for elem in seq)

	hash_list = [list(mapper(item, k, k_values)) for item in listsOfItems]
	approx = sum(min(seq[i] for seq in hash_list) for i in xrange(k)) / k
	estimate = round(1 / approx - 1, 3)
	return estimate


def calculateEmpiricalAccuracy(items, estimate):
	'''
	This function will return the difference between the estimate and the actual number of distinct
	elements in items

	items - a sequence of elements
	estimate - a number that represents the estimate (the answer from the function above)
	'''
	all_items = []
	[all_items.extend(seq) for seq in items]
	unique = sum(1 for elem in set(all_items))
	output = abs(unique - estimate)
	return output

#===============================================================================

__all__ = ['estimateDistinctElements', 'estimateDistinctElementsParallel', 'calculateEmpiricalAccuracy']

def main():
	est = estimateDistinctElementsParallel([['pizza', 'pizza', 'zing'], ['sis', 'boom', 'bah', 'humbug']], 10000)
	return calculateEmpiricalAccuracy([['pizza', 'pizza', 'zing'], ['sis', 'boom', 'bah', 'humbug']], est)

if __name__ == '__main__':
	print main()