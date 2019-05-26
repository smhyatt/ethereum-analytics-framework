ccclean:
	rm -rf database/CC/*
	-mkdir -p database/CC
	echo '' > database/CC/seenfiles.txt
	echo '' > database/CC/timespan.txt
	echo '' > database/CC/multiout.out
	python CCsetup.py

invokesclean:
	rm -rf database/transactions/*
	-mkdir -p database/transactions
	echo '' > database/transactions/seenfiles.txt
	echo '' > database/transactions/timespan.txt
	echo '' > database/transactions/multiout.out
	python invokessetup.py

cleanall: ccclean invokesclean

createinvokes1:
	python -u invokesCreator.py 0 >> database/transactions/multiout.out &
	python -u invokesCreator.py 1 >> database/transactions/multiout.out &
	python -u invokesCreator.py 2 >> database/transactions/multiout.out & 
	python -u invokesCreator.py 3 >> database/transactions/multiout.out &
	python -u invokesCreator.py 4 >> database/transactions/multiout.out &

createinvokes2:	
	python -u invokesCreator.py 5 >> database/transactions/multiout.out & 
	python -u invokesCreator.py 6 >> database/transactions/multiout.out & 
	python -u invokesCreator.py 7 >> database/transactions/multiout.out &
	python -u invokesCreator.py 8 >> database/transactions/multiout.out & 
	python -u invokesCreator.py 9 >> database/transactions/multiout.out & 
	
createinvokes3:
	python -u invokesCreator.py 10 >> database/transactions/multiout.out &
	python -u invokesCreator.py 11 >> database/transactions/multiout.out & 	
	python -u invokesCreator.py 12 >> database/transactions/multiout.out &
	python -u invokesCreator.py 13 >> database/transactions/multiout.out & 
	python -u invokesCreator.py 14 >> database/transactions/multiout.out &

createcc:
	python -u CCcreator.py >> database/CC/multiout.out &

numclusters:
	./numclusters.sh

# Remember to add the number of clusters to the bash script.
clusters:
	./clusters.sh	

treesetup:
	rm -rf database/framework/*
	-mkdir -p database/framework
	python getinvokessetup

invokestree:
	python -u invokes.py

segmentedinvokestree:
	python -u segmentedInvokes.py


# filldbs: createinvokes createcc

# cleanrun: cleanall filldbs
