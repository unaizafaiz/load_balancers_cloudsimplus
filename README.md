### CS 441 Course Project - Create and evaluate CloudSim Plus simulations that use different load balancing algorithms
### Submission By - Unaiza Faiz, Tanveer Shaikh, Joylyn Lewis 

### Details of important files included in the repository:
- /src/main/resources folder contains the files **..............conf contains the configuration settings for running the simulation  
- /src/main/scala/com/cloudsimplus/app/RandomLoadBalancer.scala contains the implementation of the Random algorithm
- /src/main/scala/com/cloudsimplus/app/RoundRobinLoadBalancer.scala contains the implementation of the Round Robin algorithm  
- /src/main/scala/com/cloudsimplus/app/HorizontalVmScalingLB.scala contains the implementation of the Horizontal VM Scaling algorithm  
- /src/test/scala/com/cloudsimplus/app/..........................scala contains the unit tests written for the application
- 'Screenshots_ .........'  provides screenshots of the sbt clean compile run, test and assembly jar file commands along with the simulation results of the three load balancing algorithms

**For Building the Docker image:**

Command to build image: **docker build -f Dockerfile -t dockerchess .**  
Command to run the image: **docker run -p 8080:8080 dockerchess**  

The generated Docker image hs been pushed to the Docker Hub public repository: **https://cloud.docker.com/repository/docker/joylyn133/joylyn_lewis_hw4_dockerimage**  
Account name: **joylyn133**  
Repository name: **joylyn_lewis_hw4_dockerimage**  
Tag name: **hw4**  


### How to run the code:




### Unit Tests included in the code:



### Approach towards designing the simulation:
The goal of this course project is to simulate cloud environments in CloudSim Plus using different load balancers in a network cloud architecture. The evaluation of the load balancers is based on the time and cost of processing the cloudlets which are submitted dynamically in the simulation. 

We use three different load balancers in our simulation:
- Random: Random is a static algorithm that is the fastest and simplest to implement. It uses a random number generator to assign cloudlets to VMs. We use the Random algorithm as the baseline for evaluating the simulation.
- Round Robin: Round robin is another static load balancing algorithm where each task on the server will be given an equal interval of time and an equal share of all resources in sequence.
- Horizontal VM Scaling: Horizontal VM Scaling is a dynamic load balancing algorithm that allows defining the condition to identify an overloaded VM, which in our case is based on current CPU utilization exceeding 70%, which will result in the creation of additional VMs.

As part of our evaluation, we assume the below Null and Alternate Hypotheses:

**Null Hypothesis:** Horizontal VM Scaling algorithm performs better than the Random and Round Robin algorithms  
**Alternate Hypothesis:** No improvement in performance can be seen with Horizontal VM Scaling algorithm as compared with the Random and Round Robin algorithms  

The simulations are carried out on the same below Cloud Network Architecture:  
- Number of Datacenters:  
- Number of VMs:  
- Number of Hosts:  
- Number of Cloudlets:  

**Evalution of the simulation:**  
  

**Results using Random alogrithm:**  


**Results using Round Robin algorithm:**  


**Results using Horizontal VM Scaling algorithm:**  

**Pros and Cons of our approach:**
**Pros:**  
- We apply the theoretical concepts learnt as part of CS 441 course in designing the network architecture and in developing our Null Hypothesis  
- Our statistical results help in proving the hypothesis

**Cons:**  
- The network architecture assumed here is on a comparatively lower scale when considered from a real world cloud computing environment. However, we limited our scaling to a basic level due to the challenges enlisted below.

**Challenges faced**
- We attempted scaling our basic architecture to assume higher number of Datacenters, VMs, Hosts along with higher number of dynamic cloudlets. While we were able to achieve scaling with the simple Cloudlet tasks designed 
for Clousim Plus, we faced dynamic VM allocation exceptions due to transfer of packets between the tasks designed for Network Cloudlets. As using Network Cloudlets was a pre-requisite for the project, we performed our simulation on a
limited scale network cloud environment.

**Conclusion:**  
Based on our performance evaluation, we see an improvement in performance in terms of cost and processing time when a dynamic algorithm like Horizontal VM Scaling is used vs other simple statis algorithms like Random and Round Robin.







