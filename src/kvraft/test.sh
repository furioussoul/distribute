export GOPATH="/Users/szj/go"
export PATH="$PATH:/usr/local/Cellar/go/1.13.5/libexec"

rm -rf /workspace/res 1>/dev/null 2>&1
mkdir /workspace/res 1>/dev/null 2>&1
set int j = 0
for ((i = 0; j < 25; i++))
do
    for ((c = $((i*10)); c < $(( (i+1)*10)); c++))
    do
         (go test -run TestSnapshotUnreliableRecoverConcurrentPartitionLinearizable3B) &> /workspace/res/$c.txt &
    done

    sleep 40

    if grep -nr "FAIL.*raft.*" /workspace/res; then
        echo "fail"
    fi

done