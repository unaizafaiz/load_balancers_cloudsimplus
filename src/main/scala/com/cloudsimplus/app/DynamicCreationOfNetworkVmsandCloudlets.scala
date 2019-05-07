package com.cloudsimplus.app

import org.cloudbus.cloudsim.allocationpolicies.VmAllocationPolicySimple
import org.cloudbus.cloudsim.brokers.DatacenterBroker
import org.cloudbus.cloudsim.brokers.DatacenterBrokerSimple
import org.cloudbus.cloudsim.cloudlets.Cloudlet
import org.cloudbus.cloudsim.cloudlets.CloudletSimple
import org.cloudbus.cloudsim.core.CloudSim
import org.cloudbus.cloudsim.datacenters.Datacenter
import org.cloudbus.cloudsim.datacenters.DatacenterSimple
import org.cloudbus.cloudsim.hosts.Host
import org.cloudbus.cloudsim.hosts.HostSimple
import org.cloudbus.cloudsim.provisioners.PeProvisionerSimple
import org.cloudbus.cloudsim.provisioners.ResourceProvisionerSimple
import org.cloudbus.cloudsim.resources.Pe
import org.cloudbus.cloudsim.resources.PeSimple
import org.cloudbus.cloudsim.schedulers.cloudlet.CloudletSchedulerSpaceShared
import org.cloudbus.cloudsim.schedulers.vm.VmSchedulerTimeShared
import org.cloudbus.cloudsim.utilizationmodels.UtilizationModel
import org.cloudbus.cloudsim.utilizationmodels.UtilizationModelFull
import org.cloudbus.cloudsim.vms.Vm
import org.cloudbus.cloudsim.vms.VmSimple
import org.cloudsimplus.builders.tables.CloudletsTableBuilder
import org.cloudsimplus.listeners.CloudletVmEventInfo
import org.cloudbus.cloudsim.vms.network.NetworkVm
import org.cloudbus.cloudsim.cloudlets.network._
import org.cloudbus.cloudsim.hosts.network.NetworkHost
import org.cloudsimplus.listeners.EventListener
import java.util

import org.cloudbus.cloudsim.datacenters.network.NetworkDatacenter
import org.cloudbus.cloudsim.network.switches.EdgeSwitch

object DynamicCreationOfNetworkVmsandCloudlets {
  var numberOfCreatedNetworkCloudlets: Int = 0
  private var numberOfCreatedNetworkVms: Int = 0
  private var broker0: DatacenterBrokerSimple = null
  //private var cloudletList: List[Cloudlet] = List()
  //private var vmList: List[Vm] = List()

  private var networkVmList = new util.ArrayList[NetworkVm]
  private var networkCloudletList = new util.ArrayList[NetworkCloudlet]
  val simulation = new CloudSim()
  val NETCLOUDLET_EXECUTION_TASK_LENGTH = 4000
  val NETCLOUDLET_RAM = 100
  val NUMBER_OF_PACKETS_TO_SEND = 100
  val PACKET_DATA_LENGTH_IN_BYTES = 1000

  def createNetworkHost() : NetworkHost = {
    val mips = 1000   // capacity of each CPU core (in Million Instructions per Second)
    val ram = 2048        // host memory (Megabyte)
    val storage = 1000000     // host storage (Megabyte)
    val bw = 10000 //in Megabits/s
    val numberOfPes = 8

    val peList = new util.ArrayList[Pe]
    val peIds: Array[Int] = 0 to numberOfPes toArray

    peIds.foreach(_ =>
      peList.add(new PeSimple(mips, new PeProvisionerSimple()))
    )
    val networkHost =  new NetworkHost(ram, bw, storage, peList)
    networkHost.setRamProvisioner(new ResourceProvisionerSimple)
      .setBwProvisioner(new ResourceProvisionerSimple)
      .setVmScheduler(new VmSchedulerTimeShared)

    return networkHost

  }

  def createNetworkVm(broker: DatacenterBroker): NetworkVm = {
    val mips: Long = 1000
    val storage: Long = 10000     // vm image size (Megabyte)
    val ram: Int = 512      // vm memory (Megabyte)
    val bw: Long = 1000     // vm bandwidth (Megabits/s)
    val pesNumber: Int = 1  // number of CPU cores

    val createdNetworkVm: NetworkVm = new NetworkVm(numberOfCreatedNetworkVms, mips, pesNumber)
    createdNetworkVm.setRam(ram)
      .setBw(bw)
      .setSize(storage)
      .setCloudletScheduler(new CloudletSchedulerSpaceShared)

    numberOfCreatedNetworkVms += 1

    return createdNetworkVm
  }

  def createNetworkCloudlet(broker: DatacenterBroker, vm: NetworkVm): NetworkCloudlet = {
    val length: Long = 10000                        //in Million Structions (MI)
    val fileSize: Long = 300                        //Size (in bytes) before execution
    val outputSize: Long = 300                      //Size (in bytes) after execution
    val numberOfCpuCores: Int = vm.getNumberOfPes.toInt //cloudlet will use all the VM's CPU cores

    //Defines how CPU, RAM and Bandwidth resources are used
    //Sets the same utilization model for all these resources.
    val utilization: UtilizationModel = new UtilizationModelFull

    val createdNetworkCloudletSimple = new NetworkCloudlet(numberOfCreatedNetworkCloudlets, length, numberOfCpuCores)
    createdNetworkCloudletSimple.setFileSize(fileSize)
      .setOutputSize(outputSize)
      .setUtilizationModel(utilization)
      .setVm(vm)

    numberOfCreatedNetworkCloudlets += 1

    return createdNetworkCloudletSimple
  }

  def createNetworkDatacenter(simulation: CloudSim) : DatacenterSimple = {
    val numberOfNetworkHosts = 1
    val networkHostList = new util.ArrayList[NetworkHost]
    val networkHostIds: Array[Int] =0 to numberOfNetworkHosts toArray

    networkHostIds.foreach(_ => {
      val networkHost =createNetworkHost()
      networkHostList.add(networkHost)
    }
    )
    new NetworkDatacenter(simulation, networkHostList, new VmAllocationPolicySimple)

  }

  protected def addExecutionTask(cloudlet: NetworkCloudlet): Unit = {
    val task = new CloudletExecutionTask(cloudlet.getTasks.size, NETCLOUDLET_EXECUTION_TASK_LENGTH)
    task.setMemory(NETCLOUDLET_RAM)
    cloudlet.addTask(task)
  }

  protected def addReceiveTask(cloudlet: NetworkCloudlet, sourceCloudlet: NetworkCloudlet): Unit = {
    val task = new CloudletReceiveTask(cloudlet.getTasks.size, sourceCloudlet.getVm)
    task.setMemory(NETCLOUDLET_RAM)
    task.setExpectedPacketsToReceive(NUMBER_OF_PACKETS_TO_SEND)
    cloudlet.addTask(task)
  }

  protected def addSendTask(sourceCloudlet: NetworkCloudlet, destinationCloudlet: NetworkCloudlet): Unit = {
    val task = new CloudletSendTask(sourceCloudlet.getTasks.size)
    task.setMemory(NETCLOUDLET_RAM)
    sourceCloudlet.addTask(task)
    var i = 0
    while ( {
      i < NUMBER_OF_PACKETS_TO_SEND
    }) {
      task.addPacket(destinationCloudlet, PACKET_DATA_LENGTH_IN_BYTES)

      {
        i += 1; i - 1
      }
    }
  }

  def createAndSubmitNetworkVmsAndCloudlets(networkVmsToCreate: Int, networkCloudletsToCreateForEachVm: Int ) = {
    val newNetworkVmList = new util.ArrayList[NetworkVm]
    val newNetworkCloudletList = new util.ArrayList[NetworkCloudlet]

    //val newNetworkCloudletList = new util.List[NetworkCloudlet]

    val networkVmIds: Array[Int] = 0 to networkVmsToCreate toArray
    val networkCloudletIds: Array[Int] = 0 to networkCloudletsToCreateForEachVm toArray


    networkVmIds.foreach(_ => {
      val vm = createNetworkVm(broker0)
      newNetworkVmList.add(vm)
      networkCloudletIds.foreach( _ => {
        val networkCloudlet: NetworkCloudlet = createNetworkCloudlet(broker0, vm)
        newNetworkCloudletList.add(networkCloudlet)
      }
      )
    }
    )

//    newNetworkCloudletList.foreach(networkCloudlet: NetworkCloudlet => {

//    })

    for (cloudlet <- newNetworkCloudletList) {
      addExecutionTask(cloudlet)
      //NetworkCloudlet 0 waits data from other Cloudlets
      if (cloudlet.get == 0)
      {
        /*
                       If there are a total of N Cloudlets, since the first one receives packets
                       from all the other ones, this for creates the tasks for the first Cloudlet
                       to wait packets from N-1 other Cloudlets.
                        */ var j = 1
        while ( {
          j < networkCloudletList.size
        }) {
          addReceiveTask(cloudlet, networkCloudletList.get(j))

          {
            j += 1; j - 1
          }
        }
      }
      else { //The other NetworkCloudlets send data to the first one
        addSendTask(cloudlet, networkCloudletList.get(0))
      }
    }



    networkVmList.addAll(newNetworkVmList)
    networkCloudletList.addAll(newNetworkCloudletList)
    broker0.submitVmList(newNetworkVmList)
    broker0.submitCloudletList(newNetworkCloudletList)



  }

  def submitNewNetworkVmsAndCloudletsToBroker(eventInfo: CloudletVmEventInfo) = {
    val numberOfNewNetworkVms = 2
    val numberOfNetworkCloudletsByVm = 4
    System.out.println("\n\t# Cloudlet %d finished. Submitting %d new VMs to the broker\n", eventInfo.getCloudlet.getId, numberOfNewNetworkVms)

    createAndSubmitNetworkVmsAndCloudlets(numberOfNewNetworkVms, numberOfNetworkCloudletsByVm)
  }

//  def createNetwork(networkDatacenter: NetworkDatacenter) = {
//    val numberOfEdgeSwitches = 1
//    val edgeSwitches = new util.ArrayList[EdgeSwitch]
//
//    val edgeSwitchesLength: Array[Int] = 0 to numberOfEdgeSwitches toArray
//
//    edgeSwitchesLength.foreach(_ => {
//      val newEdgeSwitch = new EdgeSwitch(simulation, networkDatacenter)
//      edgeSwitches.add(newEdgeSwitch)
//      networkDatacenter.addSwitch(newEdgeSwitch)
//    })
//  }

  def main(args: Array[String]): Unit = {
    System.out.println("Starting NetworkDynamicCreationOfVmsAndCloudlets")

    //val simulation = new CloudSim()
    val networkDatacenter0 = createNetworkDatacenter(simulation)



    /*Creates a Broker accountable for submission of VMs and Cloudlets on behalf of a given cloud user (customer).*/
    broker0 = new DatacenterBrokerSimple(simulation)

    val networkVmsToCreate = 1
    val networkCloudletsToCreateByVm = 2
    //val vmList = new util.ArrayList[Vm]
    //val cloudletList = new util.ArrayList[Cloudlet]

    createAndSubmitNetworkVmsAndCloudlets(networkVmsToCreate, networkCloudletsToCreateByVm)

    /* Assigns an EventListener to be notified when the first Cloudlets finishes executing
        * and then dynamically create a new list of VMs and Cloudlets to submit to the broker.*/

    val cloudlet0: NetworkCloudlet = networkCloudletList.get(0);


    cloudlet0.addOnFinishListener(submitNewNetworkVmsAndCloudletsToBroker)

    /* Starts the simulation and waits all cloudlets to be executed. */
    simulation.start

    /*Prints results when the simulation is over
        (you can use your own code here to print what you want from this cloudlet list)*/

    val finishedCloudlets = broker0.getCloudletFinishedList
    new CloudletsTableBuilder(finishedCloudlets).build()
    System.out.println(getClass.getSimpleName + " finished!")


  }



}
