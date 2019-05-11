package com.cloudsimplus.app

import java.util

import org.cloudbus.cloudsim.cloudlets.network.NetworkCloudlet


object RandomLoadBalancer {
  def main(args: Array[String]): Unit = {
    new RandomLoadBalancer
  }
}

class RandomLoadBalancer extends NetworkAbstract {

  /**
    * Creates a list of {@link NetworkCloudlet} that together represents the
    * distributed processes of a given fictitious application.
    *
    * Assigns the cloudlet created to a random virtual machine
    *
    * @return the list of create NetworkCloudlets
    */
   def createNetworkCloudlets = {
      logger.info("Create list of network cloudlets and assign VM randomly")

     /*variable is var because number of cloudlets to be created will be assigned based on
      whether it is the initial creation or cloudlet being created at runtime*/
    var numberOfCloudlets = 0

    if(cloudletList.size()==0) {
      numberOfCloudlets = NetworkAbstract.INITIAL_CLOUDLETS
    }else{
      numberOfCloudlets = NetworkAbstract.DYNAMIC_CLOUDLETS_AT_A_TIME
    }

     //initialising variables
    val networkCloudletList = new util.ArrayList[NetworkCloudlet](numberOfCloudlets)
    cloudletlistsize = cloudletList.size()
    val range = 0 until numberOfCloudlets
    val r = new scala.util.Random

    range.foreach { _ =>
      if (cloudletlistsize < NetworkAbstract.NUMBER_OF_CLOUDLETS) {
        //random vm assignment
        val cloudlet = createNetworkCloudlet(vmList.get(r.nextInt(numberOfCloudlets)))

        //Add cloudlet to global list and local list
        cloudletList.add(cloudlet)
        networkCloudletList.add(cloudlet)
        cloudletlistsize = cloudletList.size()
      }
    }

     //Create tasks for the new cloudlets created
    createTasksForNetworkCloudlets(networkCloudletList)

     //return newly created cloudlets
    networkCloudletList
  }

}
