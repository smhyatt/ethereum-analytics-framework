# Guide for Running the Framework

The Ethereum data and databases are provided upon request.

## Prerequisites and Requisites
The framework is built on Python 3.7 with Anaconda^[https://www.anaconda.com/]. Run `init.py` to install additional libraries not included in Anaconda. 

## Steps of Execution
There is a Makefile containing commands to run each step. The following is the order of the program execution. 

1. Create the files containing all contract addresses and all contract addresses that are tokens, respectively. This step is essential for the framework to run, as it requires multiple checks against the information given in both files. 
    1. `command: make createcontractlist`
    2. `command: make createtokenlist`

2. Prepare and initialise the databases.
    1. `command: make cleanall`

3. Preprocessing step of creating the databases. This part is split into four, where it is suggested to run it on three servers, adding command four below, to any one of three servers. 
    1. `command: make createinvokes1`
    2. `command: make createinvokes2`
    3. `command: make createinvokes3`
    4. `command: make createcc`

4. Once the databases are finished, one can find the right number of clusters meanwhile initialising and creating Invokes tree.
    1. `command: make numclusters`
    2. `command: make treesetup`
    3. `command: make invokestree`

5. Running clustering.
    1. `command: make clusters`



