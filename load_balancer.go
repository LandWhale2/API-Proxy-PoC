package main

import (
    "fmt"
    "log"
    "net/http"
    "sync"
    "time"
)

type Node struct {
    URL           string
    APIKey        string
    MaxBPM        int
    MaxRPM        int
    currentBPM    int
    currentRPM    int
    totalRequests int64 // 총 요청 수
    totalBytes    int64 // 처리된 총 바이트 수
    mutex         sync.Mutex
}

func (n *Node) AllowRequest(size int) bool {
    n.mutex.Lock()
    defer n.mutex.Unlock()

    if n.currentRPM+1 > n.MaxRPM {
        return false
    }

    if n.currentBPM+size > n.MaxBPM {
        return false
    }

    n.currentRPM++
    n.currentBPM += size
    n.totalRequests++
    n.totalBytes += int64(size)
    return true
}

func (n *Node) ResetCounts() {
    n.mutex.Lock()
    defer n.mutex.Unlock()
    n.currentBPM = 0
    n.currentRPM = 0
}

type LoadBalancer struct {
    Nodes      []*Node
    RetryLimit int
}

func (lb *LoadBalancer) HandleRequest(w http.ResponseWriter, req *http.Request) {
    requestSize := len(req.URL.Path) // 요청 크기를 단순하게 가정함
    var lastError string
    for i := 0; i <= lb.RetryLimit; i++ {
        for _, node := range lb.Nodes {
            if node.AllowRequest(requestSize) {
                // 이부분에 실제 API 요청 구현하기, 당장 구현은 생략
                fmt.Fprintf(w, "Request directed to %s\n", node.URL)
                return
            }
        }
        lastError = "All nodes are at capacity."
        time.Sleep(time.Second) // 간단한 재시도 간격
    }
    fmt.Fprintln(w, lastError)
    http.Error(w, lastError, http.StatusServiceUnavailable)
}

func MonitorUsage(nodes []*Node) {
    for {
        time.Sleep(10 * time.Second) // 매 10초마다 사용량 출력
        for _, node := range nodes {
            // 자세한 구현은 생략하지만, 해당 부분에 AWS 로깅/모니터링 솔루션에 데이터를 기록할수있습니다.
            node.mutex.Lock()
            fmt.Printf("Node %s: %d requests, %d bytes processed\n", node.URL, node.totalRequests, node.totalBytes)
            node.mutex.Unlock()
        }
    }
}

func main() {
    nodes := []*Node{
        {URL: "http://node1.example.com", APIKey: "123456789", MaxBPM: 100, MaxRPM: 10},
        {URL: "http://node2.example.com", APIKey: "412412312", MaxBPM: 50, MaxRPM: 5},
    }

    lb := LoadBalancer{Nodes: nodes, RetryLimit: 3}

    go MonitorUsage(nodes) // 사용량 모니터링 시작

    // 매 분마다 제한을 초기화
    ticker := time.NewTicker(time.Minute)
    go func() {
        for {
            <-ticker.C
            for _, node := range nodes {
                node.ResetCounts()
            }
            log.Println("Rate limits reset.")
        }
    }()

    // HTTP 요청을 시뮬레이션
    http.HandleFunc("/", lb.HandleRequest)
    log.Println("Load Balancer is running...")
    log.Fatal(http.ListenAndServe(":5555", nil))
}
