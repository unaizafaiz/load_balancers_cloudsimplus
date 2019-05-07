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
import org.cloudsimplus.listeners.EventListener
import java.util


object DynamicCreationOfVmsAndCloudlets {

  private var numberOfCreatedCloudlets: Int = 0
  private var numberOfCreatedVms: Int = 0
  private var broker0: DatacenterBrokerSimple = null
  //private var cloudletList: List[Cloudlet] = List()
  //private var vmList: List[Vm] = List()

  private var vmList = new util.ArrayList[Vm]
  private var cloudletList = new util.ArrayList[Cloudlet]



  def createHost() : Host = {
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
    return new HostSimple(ram, bw, storage, peList)
      .setRamProvisioner(new ResourceProvisionerSimple)
      .setBwProvisioner(new ResourceProvisionerSimple)
      .setVmScheduler(new VmSchedulerTimeShared)
  }

  def createVm(broker: DatacenterBroker): Vm = {
    val mips: Long = 1000
    val storage: Long = 10000     // vm image size (Megabyte)
    val ram: Int = 512      // vm memory (Megabyte)
    val bw: Long = 1000     // vm bandwidth (Megabits/s)
    val pesNumber: Int = 1  // number of CPU cores

    val createdVm = new VmSimple(numberOfCreatedVms, mips, pesNumber)
      .setRam(ram)
      .setBw(bw)
      .setSize(storage)
      .setCloudletScheduler(new CloudletSchedulerSpaceShared)

    numberOfCreatedVms += 1

    return createdVm
  }

  def createCloudlet(broker: DatacenterBroker, vm: Vm): Cloudlet = {
    val length: Long = 10000                        //in Million Structions (MI)
    val fileSize: Long = 300                        //Size (in bytes) before execution
    val outputSize: Long = 300                      //Size (in bytes) after execution
    val numberOfCpuCores: Long = vm.getNumberOfPes //cloudlet will use all the VM's CPU cores

    //Defines how CPU, RAM and Bandwidth resources are used
    //Sets the same utilization model for all these resources.
    val utilization: UtilizationModel = new UtilizationModelFull

    val createdCloudletSimple = new CloudletSimple(numberOfCreatedCloudlets, length, numberOfCpuCores)
      .setFileSize(fileSize)
      .setOutputSize(outputSize)
      .setUtilizationModel(utilization)
      .setVm(vm)

    numberOfCreatedCloudlets += 1

    return createdCloudletSimple
  }

  def createDatacenter(simulation: CloudSim) : DatacenterSimple = {
    val numberOfHosts = 1
    val hostList = new util.ArrayList[Host]
    val hostIds: Array[Int] =0 to numberOfHosts toArray

    hostIds.foreach(_ => {
      val host =createHost()
      hostList.add(host)
    }
    )
    new DatacenterSimple(simulation, hostList, new VmAllocationPolicySimple)
  }

  def createAndSubmitVmsAndCloudlets(vmsToCreate: Int, cloudletsToCreateForEachVm: Int ) = {
    val newVmList = new util.ArrayList[Vm]
    val newCloudletList = new util.ArrayList[Cloudlet]

    val vmIds: Array[Int] = 0 to vmsToCreate toArray
    val cloudletIds: Array[Int] = 0 to cloudletsToCreateForEachVm toArray


    vmIds.foreach(_ => {
      val vm = createVm(broker0)
      newVmList.add(vm)
      cloudletIds.foreach( _ => {
        val cloudlet: Cloudlet = createCloudlet(broker0, vm)
        newCloudletList.add(cloudlet)
      }

      )
    }


    )


    vmList.addAll(newVmList)
    cloudletList.addAll(newCloudletList)
    broker0.submitVmList(newVmList)
    broker0.submitCloudletList(newCloudletList)



  }

  def submitNewVmsAndCloudletsToBroker(eventInfo: CloudletVmEventInfo) = {
    val numberOfNewVms = 2
    val numberOfCloudletsByVm = 4
    System.out.println("\n\t# Cloudlet %d finished. Submitting %d new VMs to the broker\n", eventInfo.getCloudlet.getId, numberOfNewVms)

    createAndSubmitVmsAndCloudlets(numberOfNewVms, numberOfCloudletsByVm)
  }

  def main(args: Array[String]): Unit = {
    System.out.println("Starting DynamicCreationOfVmsAndCloudlets")

    val simulation = new CloudSim()
    val datacenter0 = createDatacenter(simulation)



    /*Creates a Broker accountable for submission of VMs and Cloudlets on behalf of a given cloud user (customer).*/
    broker0 = new DatacenterBrokerSimple(simulation)

    val vmsToCreate = 1
    val cloudletsToCreateByVm = 2
    //val vmList = new util.ArrayList[Vm]
    //val cloudletList = new util.ArrayList[Cloudlet]

    createAndSubmitVmsAndCloudlets(vmsToCreate, cloudletsToCreateByVm)

    /* Assigns an EventListener to be notified when the first Cloudlets finishes executing
        * and then dynamically create a new list of VMs and Cloudlets to submit to the broker.*/

    val cloudlet0: Cloudlet = cloudletList.get(0);


    cloudlet0.addOnFinishListener(submitNewVmsAndCloudletsToBroker)

    /* Starts the simulation and waits all cloudlets to be executed. */
    simulation.start

    /*Prints results when the simulation is over
        (you can use your own code here to print what you want from this cloudlet list)*/

    val finishedCloudlets = broker0.getCloudletFinishedList
    new CloudletsTableBuilder(finishedCloudlets).build()
    System.out.println(getClass.getSimpleName + " finished!")


  }

}
