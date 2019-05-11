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
    * @return the list of create NetworkCloudlets
    */
   def createNetworkCloudlets = {
    var numberOfCloudlets = 0

    if(cloudletList.size()==0) {
      numberOfCloudlets = NetworkAbstract.INITIAL_CLOUDLETS
    }else{
      numberOfCloudlets = NetworkAbstract.DYNAMIC_CLOUDLETS_AT_A_TIME
    }

    val networkCloudletList = new util.ArrayList[NetworkCloudlet](numberOfCloudlets)
    //val selectedVms = randomlySelectVmsForApp(broker, numberOfCloudlets)
    cloudletlistsize = cloudletList.size()
    var range = 0 until numberOfCloudlets
    val r = new scala.util.Random
    range.foreach { _ =>
      if (cloudletlistsize < NetworkAbstract.NUMBER_OF_CLOUDLETS) {
        val cloudlet = createNetworkCloudlet(vmList.get(r.nextInt(numberOfCloudlets)))
        cloudletList.add(cloudlet)
        networkCloudletList.add(cloudlet)
        cloudletlistsize = cloudletList.size()
      }
    }
    broker.submitCloudletList(networkCloudletList)
    createTasksForNetworkCloudlets(networkCloudletList)
    networkCloudletList
  }

}
