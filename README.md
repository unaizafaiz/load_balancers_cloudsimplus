### CS 441 Course Project - Create and evaluate CloudSim Plus simulations that use different load balancing algorithms
### Submission By - Unaiza Faiz, Tanveer Shaikh, Joylyn Lewis 


### How to run the code:
1. Clone the repository to your local machine using 
``git clone git@bitbucket.org:unaizafaiz/unaiza_faiz_project.git ``
2. Go to the cloned repository location on your terminal and compile the program using
``sbt clean compile run``
3. When prompted to enter a number to select main class. Enter the number of the desired load balancer class you want to run. 
4. The output prints a table of results for execution of 500 cloudlets. 

**For Building the Docker image:**

**Locally:**
Command to build image: **docker build -f Dockerfile -t docker-cloudsim-plus .**  
Command to run the image: **docker run -it docker-cloudsim-plus**  

The generated Docker image has been pushed to the Docker Hub public repository: **https://cloud.docker.com/repository/docker/unaizafaiz/unaiza_faiz_project**  
Account name: **unaizafaiz**  
Repository name: **unaiza_faiz_project**  
Tag name: **cloudsim-plus**  

#### Steps to run docker image from hub
1. Pull the image using: `docker pull unaizafaiz/unaiza_faiz_project:cloudsim-plus`
2. Run the image using: `docker run -it unaizafaiz/unaiza_faiz_project:cloudsim-plus`


#### Structure of the project folder:
- /src/main/resources folder contains the files:
    - default.conf that contains the configuration settings for running the simulation  
    - PlanetLab trace file - used to test CPU utilization model using trace files
- /src/main/scala/com/cloudsimplus/app:
    - NetworkAbstract.scala contains an abstract class that defines network cloud simulation methods and must be extended to implement different load balancers
    - RandomLoadBalancer.scala contains the implementation of the Random algorithm (extend NetworkVM)
    - RoundRobinLoadBalancer.scala contains the implementation of the Round Robin algorithm  (extends NetworkVM)
    - HorizontalVmScalingLB.scala contains the implementation of the Horizontal VM Scaling algorithm  
- /src/main/java
    - org.cloudbus.cloudsim.utilzationmodels/UtilizationModelHalf.java creates a CPU utilization model that uses 50% of the CPU resources at a time
    - org.cloudsimplus.builders.tables/CloudletsTableBuilderWithCost.java extend the cloudsimplus CloudletsTableBuilder to print the results with total cost for each cloudlet

#### Approach towards designing the simulation:
The goal of this course project is to simulate cloud environments in CloudSim Plus using different load balancers in a network cloud architecture. The evaluation of the load balancers is based on the time and cost of processing the cloudlets which are submitted dynamically in the simulation. 

We use three different load balancers in our simulation:  
- Random: Random is a static algorithm that uses a random number generator to assign cloudlets to VMs. We use the Random algorithm as the baseline for evaluating the simulation.  
- Round Robin: Round robin is another static load balancing algorithm where each cloudlet will be assigned to VM in a round - robin sequence. Each cloudlet thus will be given an equal interval of time and an equal share of all resources in sequence.  
- Horizontal VM Scaling: Horizontal VM Scaling is a dynamic load balancing algorithm that allows defining the condition to identify an overloaded VM, which in our case is based on current CPU utilization exceeding 70%, which will result in the creation of additional VMs.  

As part of our evaluation, we assume the below Null and Alternate Hypotheses:

**Null Hypothesis:** Horizontal VM Scaling algorithm performs better than the Random and Round Robin algorithms  
**Alternate Hypothesis:** No improvement in performance can be seen with Horizontal VM Scaling algorithm as compared with the Random and Round Robin algorithms  

**Architecture of the Cloud Network**
The file 'CloudSimPlusArchitecture' contains the diagram of the cloud network architecture.

The simulations are carried out Cloud Network Architecture with following characteristics:  
- Number of Datacenters:  1
- Number of VMs:  750
- Number of Hosts:  1000
- Number of Cloudlets:  500


**Evalution of the simulation:**  

The results of the simulation are in the file [results.xlsx](./results.xlsx) 

We consider Cost vs Length while performing statistical regression.
  
We found that the p-value for Horizontal VM Scaling to be 0, when compared to RoundRobin and random load balancers. 
This shows statistical significance.
  

**Results using the three alogrithms:**  
Random algorithm took higher time to execute when compared to other two algorithm. It took >3000 seconds to complete executing 500 cloudlets

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
Based on our performance evaluation, we see an improvement in performance in terms of cost and processing time when a dynamic algorithm like Horizontal VM Scaling is used vs other simple static algorithms like Random and Round Robin.







