#!/bin/zsh
testDir="testLog"
if [ ! -d ${testDir} ]; then
	mkdir ${testDir}
fi
for i in $(seq 1 20)
do
	echo "round $i"
	make project2c  > ${testDir}/"test-${i}.log"
done
echo "done"
