package com.cloudsimplus.app

import java.util

import org.cloudbus.cloudsim.cloudlets.network.NetworkCloudlet


/**
  * Implementation object to run the round robin load balancer
  */
object RoundRobinLoadBalancer {
  def main(args: Array[String]): Unit = {
    new RoundRobinLoadBalancer
  }
}

class RoundRobinLoadBalancer extends NetworkAbstract{

  /**
    * Creates a list of {@link NetworkCloudlet} that together represents the
    * distributed processes of a given fictitious application.
    *
    * @return the list of create NetworkCloudlets
    */
   def createNetworkCloudlets: util.ArrayList[NetworkCloudlet] = {
    var numberOfCloudlets = 0

    if(cloudletList.size()==0) {
      numberOfCloudlets = NetworkAbstract.INITIAL_CLOUDLETS
    }else{
      numberOfCloudlets = NetworkAbstract.DYNAMIC_CLOUDLETS_AT_A_TIME
    }

    val networkCloudletList = new util.ArrayList[NetworkCloudlet](numberOfCloudlets)

    cloudletlistsize = cloudletList.size()
    var range = 0 until numberOfCloudlets
    range.foreach { _ =>
      if(cloudletlistsize < NetworkAbstract.NUMBER_OF_CLOUDLETS) {
        val vmId = cloudletlistsize % NetworkAbstract.NUMBER_OF_VMS
        val cloudlet = createNetworkCloudlet(vmList.get(vmId))
        cloudletList.add(cloudlet)
        networkCloudletList.add(cloudlet)
        cloudletlistsize = cloudletList.size()
      }
    }
    createTasksForNetworkCloudlets(networkCloudletList)
    networkCloudletList
  }

}
