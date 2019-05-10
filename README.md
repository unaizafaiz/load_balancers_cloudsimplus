### CS 441 Course Project - Create and evaluate CloudSim Plus simulations that use different load balancing algorithms
### Submission By - Unaiza Faiz, Tanveer Shaikh, Joylyn Lewis 

### Details of important files included in the directory structure:
- /src/main/resources folder contains the files **.........conf



### How to run the code:




### Unit Tests included in the code:



### Approach towards designing the simulations:
The goal of this course project is to simulate cloud environments using different load balancers in a network architecture. The evaluation of the load balancers is based on the time and cost of processing the cloudlets which are submitted dynamically in the simulation.  
We use three different load balancers in our simulation:
- Random: Random is a static algorithm that is the fastest and simplest to implement. It uses a random number generator to assign cloudlets to VMs. We use the Random algorithm as the baseline for evaluating the simulation.
- Round Robin: Round robin is another static load balancing algorithm where each task on the server will be given an equal interval of time and an equal share of all resources in sequence.
- Horizontal VM Scaling:

Null Hypothesis: Horizontal VM Scaling algorithm performs better than the Random and Round Robin algorithms
Alternate Hypothesis: No improvement in performance can be seen with Horizontal VM Scaling algorithm as compared with the Random and Round Robin algorithms



