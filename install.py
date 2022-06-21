
import collections 

l1 = ['sa', 'ki', 'pi','po'] 
l2 = ['ki', 'po','pi', 'sa'] 


if collections.Counter(l1) == collections.Counter(l2):
    print (collections.Counter(l1) == collections.Counter(l2)) 
else: 
    print ("no") 
