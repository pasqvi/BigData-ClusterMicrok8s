# Big Data Project

## Cluster Configuration — MicroK8s
This repository provides a comprehensive guide for setting up a MicroK8s cluster for Big Data workloads.

---

### Prerequisites
Before you start, make sure you have:

#### Master Node Setup
- A **PC with ARM64 architecture** (e.g., any Apple MacBook with M1–M5 chips)
    - Install a Virtual Machine running **Ubuntu 25.04**
    - Configure the Virtual Machine network settings to use **Bridged** mode

#### Slave Nodes Setup *(Optional)*
- One or more additional PCs (any architecture) for multi-node setups
    - Install a Virtual Machine running **Ubuntu 25.04**
    - Configure the Virtual Machine network settings to use **Bridged** mode

> [!WARNING] 
> Sufficient **RAM** and **CPU cores** for cluster operations

---

### Benchmark Results
As expected, the parallelized execution of the task demonstrated significant improvements in efficiency.

| # Worker Nodes | Execution Time |
|:---------------:|:---------------:|
| 1 | 72s |
| 2 | 39s |
| 3 | 28s |

> Adding more worker nodes reduced execution time noticeably, confirming the benefits of parallelization.

---

### Developers
- **Nicola Figus**  
- **Francesco Pasqui**



