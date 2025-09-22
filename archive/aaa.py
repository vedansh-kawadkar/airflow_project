# your code goes here

def pfx(arr, n):
	pfx_dict = {}
	pfx_sum = [0]*(n)
	pfx_sum[0] = arr[0]
	for i in range(1, n):
		pfx_sum[i] = pfx_sum[i-1]+arr[i]
		
	for n in pfx_sum:
		pfx_dict[n] = pfx_dict.get(n, 0)+1
		
	return pfx_sum, pfx_dict
	

def func(arr, n, k):
	pfx_sum, pfx_dict = pfx(arr, n)
	
	count = 0
	
	for i in range(n):
		j = pfx_sum[i]-k
		count += pfx_dict.get(j, 0)
		
	return count
	
	
print(func([1, 0, 1, 2, 10, 5], 6, 3))