ccclean:
	mkdir -p database/CC
	rm -r database/CC 
	echo '' > database/CC/seenfiles.txt
	echo '' > database/CC/timespan.txt
	echo '' > database/CC/multiout.out
	python CCsetup.py

invokesclean:
	mkdir -p database/transactions
	rm -r database/transactions
	echo '' > database/transactions/seenfiles.txt
	echo '' > database/transactions/timespan.txt
	echo '' > database/transactions/multiout.out
	python invokessetup.py

cleanall: CCclean invokesclean

createinvokes: 
	python -u invokesCreator.py 0 >> database/transactions/multiout.out &
	python -u invokesCreator.py 1 >> database/transactions/multiout.out &
	python -u invokesCreator.py 2 >> database/transactions/multiout.out &
	python -u invokesCreator.py 3 >> database/transactions/multiout.out &
	python -u invokesCreator.py 4 >> database/transactions/multiout.out &
	python -u invokesCreator.py 5 >> database/transactions/multiout.out &
	python -u invokesCreator.py 6 >> database/transactions/multiout.out &
	python -u invokesCreator.py 7 >> database/transactions/multiout.out &
	python -u invokesCreator.py 8 >> database/transactions/multiout.out &
	python -u invokesCreator.py 9 >> database/transactions/multiout.out &
	python -u invokesCreator.py 10 >> database/transactions/multiout.out &
	python -u invokesCreator.py 11 >> database/transactions/multiout.out &
	python -u invokesCreator.py 12 >> database/transactions/multiout.out &
	python -u invokesCreator.py 13 >> database/transactions/multiout.out &
	python -u invokesCreator.py 14 >> database/transactions/multiout.out &
	python -u invokesCreator.py 15 >> database/transactions/multiout.out &
	python -u invokesCreator.py 16 >> database/transactions/multiout.out &
	python -u invokesCreator.py 17 >> database/transactions/multiout.out &
	python -u invokesCreator.py 18 >> database/transactions/multiout.out &
	python -u invokesCreator.py 19 >> database/transactions/multiout.out &
	python -u invokesCreator.py 20 >> database/transactions/multiout.out &
	python -u invokesCreator.py 21 >> database/transactions/multiout.out &
	python -u invokesCreator.py 22 >> database/transactions/multiout.out &
	python -u invokesCreator.py 23 >> database/transactions/multiout.out &
	python -u invokesCreator.py 24 >> database/transactions/multiout.out &
	python -u invokesCreator.py 25 >> database/transactions/multiout.out &
	python -u invokesCreator.py 26 >> database/transactions/multiout.out &

createcc:
	python -u CCcreator.py >> database/CC/multiout.out &

filldbs: createinvokes createcc

cleanrun: cleanall filldbs

