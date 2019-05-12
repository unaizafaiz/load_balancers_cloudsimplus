package com.cloudsimplus.app

import java.util

import org.cloudbus.cloudsim.cloudlets.network.NetworkCloudlet


/**
  * Extends NetworkAbstract class to implement a round robin load balancer
  *
  */
object RoundRobinLoadBalancer {
  def main(args: Array[String]): Unit = {
    new RoundRobinLoadBalancer
  }
}

class RoundRobinLoadBalancer extends NetworkAbstract{

  /**
    * Creates a list of {@link NetworkCloudlet} that together represents the
    * distributed processes of a given fictitious application
    *
    * Assigns the cloudlet to a virtual machine based on round robin fashion (0,1,2,3,0,1,2,3,0....)
    *
    * @return the list of create NetworkCloudlets
    */
   def createNetworkCloudlets: util.ArrayList[NetworkCloudlet] = {

     logger.info("Create list of network cloudlets and assign VM in round robin manner")

     /*variable is var because number of cloudlets to be created will be assigned based on
      whether it is the initial creation or cloudlet being created at runtime*/
    var numberOfCloudlets = 0

    if(cloudletList.size()==0) {
      numberOfCloudlets = NetworkAbstract.INITIAL_CLOUDLETS
    }else{
      numberOfCloudlets = NetworkAbstract.DYNAMIC_CLOUDLETS_AT_A_TIME
    }

    val networkCloudletList = new util.ArrayList[NetworkCloudlet](numberOfCloudlets)
    cloudletlistsize = cloudletList.size()
    val range = 0 until numberOfCloudlets

    range.foreach { _ =>
      if(cloudletlistsize < NetworkAbstract.NUMBER_OF_CLOUDLETS) {
        //find the vmID based on round robin
        val vmId = cloudletlistsize % NetworkAbstract.NUMBER_OF_VMS
        //Create cloudlet and assign it to the vmID
        val cloudlet = createNetworkCloudlet(vmList.get(vmId))
        //Add cloudlet to global list and local list
        cloudletList.add(cloudlet)
        networkCloudletList.add(cloudlet)
        cloudletlistsize = cloudletList.size()
      }
    }

     //Create tasks for the new cloudlets created
    createTasksForNetworkCloudlets(networkCloudletList)

     //return cloudlets
    networkCloudletList
  }

}
