# Building and Running Banyan Queries

## System Requirements

### OS
Linux only(preferably ubuntu 18.04)，windows and macOS are not supported.
### GCC version
gcc7 or gcc8, other versions of gcc will cause some compilation problems

## Build the project

### (A) Install clang
Download [clang-8.0.0 pre-built binaries](
http://releases.llvm.org/8.0.0/clang+llvm-8.0.0-x86_64-linux-gnu-ubuntu-18.04.tar.xz).
```shell
tar xvJf clang+llvm-8.0.0-x86_64-linux-gnu-ubuntu-18.04.tar.xz
mv clang+llvm-8.0.0-x86_64-linux-gnu-ubuntu-18.04 clang-8.0.0
sudo mv clang-8.0.0 /usr/local
```
Open ~/.profile and add the following scripts:
```shell
export PATH=/usr/local/clang-8.0.0/bin:$PATH
export LD_LIBRARY_PATH=/usr/local/clang-8.0.0/lib:$LD_LIBRARY_PATH
```
Source the ~/.profile file:
```shell
source ~/.profile
```

### (B) Configure and build
```shell
# enter the code source dir
cd banyan
# install dependencies
sudo ./install-dependencies.sh
# configure
./configure.py --mode=release --cook fmt --compiler={g++-7, g++-8}  --c++-dialect="gnu++17" \
  --libclang-path=/usr/local/clang-8.0.0/lib --without-tests --without-demos
# build
ninja -C build/release
``` 

## Prepare the dataset
Since the data set of ldbc100 is too large, we only provide the ldbc1 dataset used in the experiments
```shell
cd banyan
cd apps/banyan_gqs/berkeley_db/db_data/ldbc1/
wget https://graphscope.oss-cn-beijing.aliyuncs.com/vldb/ldbc1_data.zip
unzip ldbc1_data.zip
mv ldbc1_data/* ./
rm ldbc1_data.zip
rm -r ldbc1_data/
```

## Run Queries

### Basic running options
```
-c{x}             # run with number of 'x' cores. (e.g. -c4)

-m{x}g            # limit the running memory within 'x' GB. (e.g. -m10g)

-dataset={d}      # use the dataset 'd', you should use 'ldbc1' here.

-batch-size={bs}  # set the data batch size of 'bs' between query operators.
                  # the default value is 64.

-param-num={pn}   # set the number of query parameters to be 'pn' when
                  # running queries, for ldbc ic query, 'pn' should <= 50, for
                  # cq queries, 'pn' should <= 10.
                  
-exec-epoch={ep}  # set the number of epochs a query will be run 
```

### Run ldbc ic queries
Enter the build dir:
```shell
cd banyan
cd build/release/apps/banyan_gqs
```
Run an ldbc ic query:
```shell
./run_ic_{id} -c{cores} -m{memory}g -dataset=ldbc1 -param-num=50 -exec-epoch={epochs}
```

### Run cq queries
cq specific running options:
```
-cq-max-si={x}                # set the max number of concurrent scope instances for
                              # each scope operator to be 'x', the default value is 10.

-cq-limit-n={n}               # set the query result limit number to be 'n' (if a
                              # query has a limit(n) operator), the default value is 10.

--cq-close-limit-cancel       # turn off the early cancellation of limit(n) operator,
                              # by default this option is set to be turned on.
                                    
--cq-close-branch-cancel      # turn off the early cancellation in a where branch,
                              # by default this option is set to be turned on.
                            
-cq-query-policy={p}          # set the scheduling policy at query level, 'p' should be
                              # one of {fifo, dfs, bfs}, the default policy is fifo.
                            
-cq-loop-policy={p}           # set the scheduling policy for loop scopes, the default
                              # policy is fifo.

-cq-loop-instance-policy={p}  # set the scheduling policy for loop scope instances,
                              # the default policy is fifo.
                             
-cq-where-policy={p}          # set the scheduling policy for where scopes, the default
                              # policy is fifo.

-cq-where-branch-policy={p}   # set the scheduling policy for where scope instances, the
                              # default policy is fifo.
```

The CQ benchmark has 6 cq queries. For cq_3, cq_4, cq_5 and cq_6, we implemented two versions 
(banyan and timely) for comparison. E.g., cq_4 (see run_cq_4) is the scoped-dataflow version 
implemented using banyan (with scoped turned on), while cq_4x (see run_cq_4x) is the timely-dataflow 
version implemented using banyan without scopes.

Run a cq query:
```shell
# enter the built dir:
cd banyan
cd build/release/apps/banyan_gqs
# run
./run_cq_{id{x}} -c{cores} -m{memory}g -dataset=ldbc1 -param-num=10 -exec-epoch={epochs} { cq options ... }
```