You need to develop all your code for all the parts of Lab 3 in the raft.go file.

After you have made your modifications, you can run all the testing code as follows:

    go test -run 3A
    go test -run 3B
    go test -run 3C    

If you wish to run individual tests, look for function names starting with `Test`, e.g., `TestInitialElection3A` in `test_test.go`. Then run the individual test as follows:

    go test -run TestInitialElection3A

You can also run the following script to estimate your lab grade:

    /cad2/ece419/bin/ece419-lab3-check [A|B|C]
