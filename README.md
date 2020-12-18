# NJU-DisSys-2020
This is the resource repository for the course Distributed System, Fall 2020, CS@NJU.



## 1. 实验分析

### Part 1 Leader Election

#### 实验需求

- First task is to fill the `RequestVoteArgs`and `RequestVoteReply` structs 补充数据结构
- Modify Make() to create a background goroutine that starts an election by sending out `RequestVote` RPC when it hasn’t heard from another peer for a while
  - You need to implement `RequestVote` RPC handler so that servers will vote for one another
- To implement heartbeats, you will need to define `AppendEntries` struct (though you will not need any real payload yet), and have the leader send them out periodically 定义`AppendEntries`数据结构
  - Also need to implement `AppendEntries` RPC handler
- make sure the election timeouts don't always fire at the same time 确保

### 实验建议

- Remember field names of any structures you will be sending over RPC must start with capital letters 通过RPC发送的域名大写
- Read and understand the paper before you start. Figure 2 in the paper may provide a good guideline. 
- Start early!

### 测试代码

主要有如下两个测试代码

#### TestInitElection：初始选举测试

```go
func TestInitialElection(t *testing.T) {
	servers := 3
	cfg := make_config(t, servers, false)
	defer cfg.cleanup()

	fmt.Printf("Test: initial election ...\n")

	// is a leader elected?
	cfg.checkOneLeader()

	// does the leader+term stay the same there is no failure?
	term1 := cfg.checkTerms()
	time.Sleep(2 * RaftElectionTimeout)
	term2 := cfg.checkTerms()
	if term1 != term2 {
		fmt.Printf("warning: term changed even though there were no failures")
	}

	fmt.Printf("  ... Passed\n")
}
```



#### TestReElection：重新选举测试