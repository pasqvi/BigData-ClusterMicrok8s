# Big Data Project

## Cluster Configuration — MicroK8s
This repository provides a comprehensive guide for setting up a MicroK8s cluster for Big Data workloads.

---

### Prerequisites
Before you start, make sure you have:

- A virtual machine running **Ubuntu 25.04**
- A **PC with ARM64 architecture** (e.g., any Apple MacBook with M1–M5 chips)
- Sufficient **RAM** and **CPU cores** for cluster operations
- *(Optional)* Two or more additional PCs (any architecture) running Ubuntu — for multi-node setups

---

### Benchmark Results
As expected, the parallelized execution of the task demonstrated significant improvements in efficiency.

| # Worker Nodes | Execution Time |
|:---------------:|:---------------:|
| 1 | 50s |
| 2 | 39s |
| 3 | 28s |

> Adding more worker nodes reduced execution time noticeably, confirming the benefits of parallelization.

---

### Developers
- **Nicola Figus**  
- **Francesco Pasqui**



