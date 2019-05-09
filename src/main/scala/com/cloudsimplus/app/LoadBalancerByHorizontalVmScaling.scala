
package com.cloudsimplus.app

import org.cloudbus.cloudsim.allocationpolicies.VmAllocationPolicySimple
import org.cloudbus.cloudsim.brokers.DatacenterBrokerSimple
import org.cloudbus.cloudsim.cloudlets.Cloudlet
import org.cloudbus.cloudsim.cloudlets.CloudletSimple
import org.cloudbus.cloudsim.core.CloudSim
import org.cloudbus.cloudsim.datacenters.DatacenterSimple
import org.cloudbus.cloudsim.distributions.UniformDistr
import org.cloudbus.cloudsim.hosts.Host
import org.cloudbus.cloudsim.hosts.HostSimple
import org.cloudbus.cloudsim.provisioners.PeProvisionerSimple
import org.cloudbus.cloudsim.provisioners.ResourceProvisionerSimple
import org.cloudbus.cloudsim.resources.Pe
import org.cloudbus.cloudsim.resources.PeSimple
import org.cloudbus.cloudsim.schedulers.cloudlet.CloudletSchedulerTimeShared
import org.cloudbus.cloudsim.schedulers.vm.VmSchedulerTimeShared
import org.cloudbus.cloudsim.utilizationmodels.UtilizationModelFull
import org.cloudbus.cloudsim.vms.Vm
import org.cloudbus.cloudsim.vms.VmSimple
import org.cloudsimplus.autoscaling.HorizontalVmScalingSimple
import org.cloudsimplus.builders.tables.CloudletsTableBuilder
import org.cloudsimplus.listeners.EventInfo
import java.util
import java.util.Comparator.comparingDouble



/**
  * Balancing the load by dynamically creating VMs,
  * according to the arrival of Cloudlets.
  * Cloudlets are {@link #createNewCloudlets(EventInfo) dynamically created and submitted to the broker
 * at specific time intervals}.
  *
  */
object LoadBalancerByHorizontalVmScaling {

  private val SCHEDULING_INTERVAL = 5
  /**
    * The interval to request the creation of new Cloudlets.
    */
  private val CLOUDLETS_CREATION_INTERVAL = SCHEDULING_INTERVAL * 2
  private val HOSTS = 50
  private val HOST_PES = 32
  private val VMS = 4
  private val CLOUDLETS = 6
  /**
    * Different lengths that will be randomly assigned to created Cloudlets.
    */
  private val CLOUDLET_LENGTHS = Array(2000, 4000, 10000, 16000, 2000, 30000, 20000)
  var createdCloudlets = 0
  var createsVms = 0
  val rand = new UniformDistr(0, LoadBalancerByHorizontalVmScaling.CLOUDLET_LENGTHS.length, seed)
  val hostList = new util.ArrayList[Host](LoadBalancerByHorizontalVmScaling.HOSTS)
  val vmList = new util.ArrayList[Vm](LoadBalancerByHorizontalVmScaling.VMS)
  val cloudletList = new util.ArrayList[Cloudlet](LoadBalancerByHorizontalVmScaling.CLOUDLETS)
  val simulation = new CloudSim()

  //var because broker0 is initialised in the initialize function()
  var broker0 = new DatacenterBrokerSimple(simulation)


  /*Enables just some level of log messages.
           Make sure to import org.cloudsimplus.util.Log;*/
  //Log.setLevel(ch.qos.logback.classic.Level.WARN);
  /*You can remove the seed parameter to get a dynamic one, based on current computer time.
          * With a dynamic seed you will get different results at each simulation run.*/ val seed = 1


  def initialize(): Unit = {
    println("VMLsearch Init")
    simulation.addOnClockTickListener(createNewCloudlets)
    createDatacenter()
    broker0 = new DatacenterBrokerSimple(simulation)

    /*
           * Defines the Vm Destruction Delay Function as a lambda expression
           * so that the broker will wait 10 seconds before destroying an idle VM.
           * By commenting this line, no down scaling will be performed
           * and idle VMs will be destroyed just after all running Cloudlets
           * are finished and there is no waiting Cloudlet. */
    broker0.setVmDestructionDelayFunction((vm: Vm) => 10.0)
    vmList.addAll(createListOfScalableVms(LoadBalancerByHorizontalVmScaling.VMS))
    println("VMLsearch", vmList)
    createCloudletList()
    broker0.submitVmList(vmList)
    broker0.submitCloudletList(cloudletList)
    simulation.start
    printSimulationResults()

  }

  def main(args: Array[String]): Unit = {
    initialize()
  }


  private def printSimulationResults(): Unit = {
    val finishedCloudlets = broker0.getCloudletFinishedList
    val sortByVmId = comparingDouble((c: Cloudlet) => c.getVm.getId)
    val sortByStartTime = comparingDouble((c: Cloudlet) => c.getExecStartTime)
    finishedCloudlets.sort(sortByVmId.thenComparing(sortByStartTime))
    new CloudletsTableBuilder(finishedCloudlets).build()
  }

  private def createCloudletList(): Unit = {
    val range = 0 until LoadBalancerByHorizontalVmScaling.CLOUDLETS
    range.foreach{_=>
      cloudletList.add(createCloudlet)
    }
  }

  /**
    * Creates new Cloudlets at every {@link #CLOUDLETS_CREATION_INTERVAL} seconds, up to the 50th simulation second.
    * A reference to this method is set as the {@link EventListener}
    * to the {@link Simulation#addOnClockTickListener(EventListener)}.
    * The method is called every time the simulation clock advances.
    *
    * @param eventInfo the information about the OnClockTick event that has happened
    */
  private def createNewCloudlets(eventInfo: EventInfo): Unit = {
    val time = eventInfo.getTime.toLong
    if (time % LoadBalancerByHorizontalVmScaling.CLOUDLETS_CREATION_INTERVAL == 0 && time <= 50) {
      val numberOfCloudlets = 4
      printf("\t#Creating %d Cloudlets at time %d.\n", numberOfCloudlets, time)
      val newCloudlets = new util.ArrayList[Cloudlet](numberOfCloudlets)
      val range = 0 until numberOfCloudlets
      range.foreach{_ => {
        val cloudlet = createCloudlet
        cloudletList.add(cloudlet)
        newCloudlets.add(cloudlet)
      }
        broker0.submitCloudletList(newCloudlets)
    }}
  }

  /**
    * Creates a Datacenter and its Hosts.
    */
  private def createDatacenter(): Unit = {
    val range = 0 until LoadBalancerByHorizontalVmScaling.HOSTS
    range.foreach{ _ =>
      hostList.add(createHost)
    }
    val dc0 = new DatacenterSimple(simulation, hostList, new VmAllocationPolicySimple)
    dc0.setSchedulingInterval(LoadBalancerByHorizontalVmScaling.SCHEDULING_INTERVAL)
  }

  private def createHost = {
    val peList = new util.ArrayList[Pe](LoadBalancerByHorizontalVmScaling.HOST_PES)
    val range = 0 until LoadBalancerByHorizontalVmScaling.HOST_PES
    range.foreach{ _ =>
      peList.add(new PeSimple(1000, new PeProvisionerSimple))
    }
    val ram = 2048 // in Megabytes
    val storage = 1000000
    val bw = 10000 //in Megabits/s
    new HostSimple(ram, bw, storage, peList).setRamProvisioner(new ResourceProvisionerSimple).setBwProvisioner(new ResourceProvisionerSimple).setVmScheduler(new VmSchedulerTimeShared)
  }

  /**
    * Creates a list of initial VMs in which each VM is able to scale horizontally
    * when it is overloaded.
    *
    * @param numberOfVms number of VMs to create
    * @return the list of scalable VMs
    * @see #createHorizontalVmScaling(Vm)
    */
  private def createListOfScalableVms(numberOfVms: Int) = {
    val newList = new util.ArrayList[Vm](numberOfVms)
    val range = 0 until numberOfVms
    range.foreach{ i =>
      val vm = createVm
      createHorizontalVmScaling(vm)
      newList.add(vm)
    }
    newList
  }

  /**
    * Creates a {@link HorizontalVmScaling} object for a given VM.
    *
    * @param vm the VM for which the Horizontal Scaling will be created
    * @see #createListOfScalableVms(int)
    */
  private def createHorizontalVmScaling(vm: Vm): Unit = {
    val horizontalScaling = new HorizontalVmScalingSimple
    horizontalScaling.setVmSupplier(() => {
      createVm
    }).setOverloadPredicate(isVmOverloaded)
    vm.setHorizontalScaling(horizontalScaling)
  }


  /**
    * A {@link Predicate} that checks if a given VM is overloaded or not,
    * based on upper CPU utilization threshold.
    * A reference to this method is assigned to each {@link HorizontalVmScaling} created.
    *
    * @param vm the VM to check if it is overloaded
    * @return true if the VM is overloaded, false otherwise
    * @see #createHorizontalVmScaling(Vm)
    */
  private def isVmOverloaded(vm: Vm) = vm.getCpuPercentUsage > 0.7

  /**
    * Creates a Vm object.
    *
    * @return the created Vm
    */

  private def createVm = {
    val id = createsVms
    createsVms +=1
    new VmSimple(id, 1000, 2).setRam(512).setBw(1000).setSize(10000).setCloudletScheduler(new CloudletSchedulerTimeShared)

  }

  private def createCloudlet = {
    val id = createdCloudlets
    createdCloudlets +=1
    //randomly selects a length for the cloudlet
    val length = LoadBalancerByHorizontalVmScaling.CLOUDLET_LENGTHS(rand.sample.toInt)
    val utilization = new UtilizationModelFull
    new CloudletSimple(id, length, 2).setFileSize(1024).setOutputSize(1024).setUtilizationModel(utilization)
  }
}
