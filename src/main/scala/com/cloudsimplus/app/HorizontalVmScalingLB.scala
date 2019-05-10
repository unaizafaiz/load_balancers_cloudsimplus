package com.cloudsimplus.app

import org.cloudbus.cloudsim.allocationpolicies.VmAllocationPolicySimple
import org.cloudbus.cloudsim.brokers.DatacenterBroker
import org.cloudbus.cloudsim.brokers.DatacenterBrokerSimple
import org.cloudbus.cloudsim.cloudlets.network._
import org.cloudbus.cloudsim.core.CloudSim
import org.cloudbus.cloudsim.datacenters.network.NetworkDatacenter
import org.cloudbus.cloudsim.hosts.Host
import org.cloudbus.cloudsim.hosts.network.NetworkHost
import org.cloudbus.cloudsim.network.switches.EdgeSwitch
import org.cloudbus.cloudsim.provisioners.PeProvisionerSimple
import org.cloudbus.cloudsim.provisioners.ResourceProvisionerSimple
import org.cloudbus.cloudsim.resources.Pe
import org.cloudbus.cloudsim.resources.PeSimple
import org.cloudbus.cloudsim.schedulers.cloudlet.{CloudletSchedulerSpaceShared, CloudletSchedulerTimeShared}
import org.cloudbus.cloudsim.schedulers.vm.VmSchedulerTimeShared
import org.cloudbus.cloudsim.utilizationmodels.UtilizationModelFull
import org.cloudbus.cloudsim.vms.network.NetworkVm
import org.cloudsimplus.builders.tables.CloudletsTableBuilder
import java.util
import java.util.ArrayList

import org.cloudbus.cloudsim.distributions.UniformDistr
import org.cloudbus.cloudsim.vms.Vm
import org.cloudsimplus.autoscaling.HorizontalVmScalingSimple
import org.cloudsimplus.listeners.EventInfo


/**
  *Load balancing by horizontal scaling of VM
  */
object HorizontalVmScalingLB {
  private val NUMBER_OF_HOSTS = 30
  private val HOST_MIPS = 1000
  private val HOST_PES = 4
  private val HOST_RAM = 512 // host memory (Megabyte)

  private val HOST_STORAGE = 1000000 // host storage

  private val HOST_BW = 10000
  private val CLOUDLET_EXECUTION_TASK_LENGTH = 2000
  private val CLOUDLET_FILE_SIZE = 1024
  private val CLOUDLET_OUTPUT_SIZE = 1024
  private val PACKET_DATA_LENGTH_IN_BYTES = 1000
  private val NUMBER_OF_PACKETS_TO_SEND = 10
  private val TASK_RAM = 5000

  private val VMS = 2
  private val CLOUDLETS = 2

  private val SCHEDULING_INTERVAL = 5
  private val CLOUDLETS_CREATION_INTERVAL = SCHEDULING_INTERVAL * 2





  /**
    * Starts the execution of the example.
    *
    * @param args command line arguments
    */
  def main(args: Array[String]): Unit = {
    new HorizontalVmScalingLB
  }

  /**
    * Adds an execution task to the list of tasks of the given
    * {@link NetworkCloudlet}.
    *
    * @param cloudlet the { @link NetworkCloudlet} the task will belong to
    */
  private def addExecutionTask(cloudlet: NetworkCloudlet): Unit = {
   // print("Executing cloudlet "+cloudlet.getId)
    val task = new CloudletExecutionTask(cloudlet.getTasks.size, CLOUDLET_EXECUTION_TASK_LENGTH)
    task.setMemory(TASK_RAM)
    cloudlet.addTask(task)
  }
}

class HorizontalVmScalingLB private() {

  /**
    * Creates, starts, stops the simulation and shows results.
    */
  println("Starting " + getClass.getSimpleName)


  var cloudletlistsize =0
  var createVms = 0
  var cloudletid = 0
  private val TIME_TO_TERMINATE_SIMULATION: Double = 1000


  val simulation: CloudSim = new CloudSim
  simulation.terminateAt(TIME_TO_TERMINATE_SIMULATION)
  simulation.addOnClockTickListener(createNetworkCloudlets)
  private var random = new UniformDistr()
  val datacenter: NetworkDatacenter = createDatacenter
  val broker: DatacenterBroker = new DatacenterBrokerSimple(simulation)
  broker.setVmDestructionDelayFunction((vm: Vm) => 1000.0)
  val vmList = new util.ArrayList[NetworkVm](HorizontalVmScalingLB.VMS)
  vmList.addAll(createListOfScalableVms(HorizontalVmScalingLB.VMS))
  val cloudletList = new util.ArrayList[NetworkCloudlet]()
  createInitialCloudlets
  broker.submitVmList(vmList)
  broker.submitCloudletList(cloudletList)
  simulation.start
  //createNetworkTask
  //Thread.sleep(500)
  showSimulationResults()

  private def showSimulationResults(): Unit = {
    val newList = broker.getCloudletFinishedList
    val range = 0 until newList.size()
    for(i<-range){
      val cloudlet = newList.get(i)
      printf("i="+i)
    }
    new CloudletsTableBuilder(newList).build()
    import scala.collection.JavaConversions._
    for (host <- datacenter.getHostList[NetworkHost]) {
      println(s"\nHost "+host.getId+" data transferred: "+host.getTotalDataTransferBytes+" bytes", host.getId, host.getTotalDataTransferBytes)
    }
    println(this.getClass.getSimpleName + " finished!")
  }

  /**
    * Creates the Datacenter.
    *
    * @return the Datacenter
    */
  private def createDatacenter = {
    val hostList = new util.ArrayList[Host]
    val range = 0 until HorizontalVmScalingLB.NUMBER_OF_HOSTS
    range.foreach(_ -> {
      val host = createHost
      hostList.add(host)
    })
    val newDatacenter = new NetworkDatacenter(simulation, hostList, new VmAllocationPolicySimple)
    newDatacenter.setSchedulingInterval(HorizontalVmScalingLB.SCHEDULING_INTERVAL)
    //createNetwork(newDatacenter)
    newDatacenter
  }

  private def createHost = {
    val peList = createPEs(HorizontalVmScalingLB.HOST_PES, HorizontalVmScalingLB.HOST_MIPS)
    new NetworkHost(HorizontalVmScalingLB.HOST_RAM, HorizontalVmScalingLB.HOST_BW, HorizontalVmScalingLB.HOST_STORAGE, peList).setRamProvisioner(new ResourceProvisionerSimple).setBwProvisioner(new ResourceProvisionerSimple).setVmScheduler(new VmSchedulerTimeShared)
  }

  private def createPEs(numberOfPEs: Int, mips: Long) = {
    val peList = new util.ArrayList[Pe]
    val range = 0 until numberOfPEs
    range.foreach(_->peList.add(new PeSimple(mips, new PeProvisionerSimple)))
    peList
  }

  /**
    * Creates internal Datacenter network.
    *
    * @param datacenter Datacenter where the network will be created
    */
  private def createNetwork(datacenter: NetworkDatacenter): Unit = {

    val numberOfEdgeSwitches = HorizontalVmScalingLB.NUMBER_OF_HOSTS-1
    val edgeSwitches: ArrayList[EdgeSwitch] = new util.ArrayList[EdgeSwitch]

    (0 to numberOfEdgeSwitches).toArray.foreach(_ => {
      val edgeSwitch = new EdgeSwitch(simulation, datacenter)
      edgeSwitches.add(edgeSwitch)
      datacenter.addSwitch(edgeSwitch)
    })

    import scala.collection.JavaConversions._
    for (host <- datacenter.getHostList[NetworkHost]) {
      val switchNum: Int = getSwitchIndex(host, edgeSwitches(0).getPorts).toInt
      edgeSwitches(switchNum).connectHost(host)
    }
  }

  def getSwitchIndex(host: NetworkHost, switchPorts: Int): Long = host.getId % Integer.MAX_VALUE.round / switchPorts

  /**
    * Creates a list of initial VMs in which each VM is able to scale horizontally
    * when it is overloaded.
    *
    * @param numberOfVms number of VMs to create
    * @return the list of scalable VMs
    * @see #createHorizontalVmScaling(Vm)
    */
  private def createListOfScalableVms(numberOfVms: Int) = {
    val newList = new util.ArrayList[NetworkVm](numberOfVms)
    val range = 0 until numberOfVms
    range.foreach{ i =>
      val vm = createVm(createVms)
      createVms+=1
      createHorizontalVmScaling(vm)
      newList.add(vm)
    }
    newList
  }

  private def createVm(id: Int) = {
    val vm = new NetworkVm(id, HorizontalVmScalingLB.HOST_MIPS, HorizontalVmScalingLB.HOST_PES)
    vm.setRam(HorizontalVmScalingLB.HOST_RAM).setBw(HorizontalVmScalingLB.HOST_BW).setSize(HorizontalVmScalingLB.HOST_STORAGE).setCloudletScheduler(new CloudletSchedulerTimeShared)
    vm
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
      createVm(createVms)
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
    * Creates a list of {@link NetworkCloudlet} that together represents the
    * distributed processes of a given fictitious application.
    *
    * @return the list of create NetworkCloudlets
    */
  private def createInitialCloudlets = {
    val numberOfCloudlets = HorizontalVmScalingLB.CLOUDLETS
    val networkCloudletList = new util.ArrayList[NetworkCloudlet](numberOfCloudlets)
    val range = 0 until HorizontalVmScalingLB.CLOUDLETS
    range.foreach{_=>
     // print("in range")
      cloudletList.add(createNetworkCloudlet)
    }

    //Creating tasks here is leading to an error as VM is not assigned to cloudlets initially
    //Uncomment to check the error

    //createNetworkTask

  }

  private def createNetworkTask(networkCloudletList: util.ArrayList[NetworkCloudlet] ) ={
    printf("Creating network tasks\n")
    //NetworkCloudlet 0 Tasks
    HorizontalVmScalingLB.addExecutionTask(networkCloudletList.get(0))
    addSendTask(networkCloudletList.get(0), networkCloudletList.get(1))
    //NetworkCloudlet 1 Tasks
    addReceiveTask(networkCloudletList.get(1), networkCloudletList.get(0))
    HorizontalVmScalingLB.addExecutionTask(networkCloudletList.get(1))
    //broker.submitCloudletList(cloudletList)
  }

  private def createTasksForNetworkCloudlets(networkCloudletList: util.ArrayList[NetworkCloudlet]): Unit = {
    import scala.collection.JavaConversions._
    for (cloudlet <- networkCloudletList) {
      HorizontalVmScalingLB.addExecutionTask(cloudlet)
      //NetworkCloudlet 0 waits data from other Cloudlets
      if (cloudlet.getId == 0) {
        /*
                       If there are a total of N Cloudlets, since the first one receives packets
                       from all the other ones, this for creates the tasks for the first Cloudlet
                       to wait packets from N-1 other Cloudlets.
                        */
        for(j<-1 until networkCloudletList.size()) {
          addReceiveTask(cloudlet, networkCloudletList.get(j))
        }
      }
      else { //The other NetworkCloudlets send data to the first one
        addSendTask(cloudlet, networkCloudletList.get(0))
      }
    }
  }

  /**
    * Creates a list of {@link NetworkCloudlet} that together represents the
    * distributed processes of a given fictitious application.
    *
    * @return the list of create NetworkCloudlets
    */
  private def createNetworkCloudlets(eventInfo: EventInfo): Unit = {
    //print("Creating new cloudlets")

    //initial cloudlets tasks created
    if(cloudletList.size()==2){
      createNetworkTask(cloudletList)
    }

    //Creating new cloudlets at intervals
    val time = eventInfo.getTime.toLong
    if (time % HorizontalVmScalingLB.CLOUDLETS_CREATION_INTERVAL == 0 && time <= 30) {
      val numberOfCloudlets = 2
      printf("\t#Creating %d Cloudlets at time %d.\n", numberOfCloudlets, time)
      val networkCloudletList = new util.ArrayList[NetworkCloudlet](numberOfCloudlets)
      val range = 0 until numberOfCloudlets
      range.foreach { _ => {
        val cloudlet = createNetworkCloudlet
        cloudletList.add(cloudlet)
        networkCloudletList.add(cloudlet)
      }
      }
      broker.submitCloudletList(networkCloudletList)


      //TO-DO: The generalise cloudlet task creation is throwing a null pointer exception, needs to be fixed
      //createTasksForNetworkCloudlets(networkCloudletList)


    }

  }


  /**
    * Creates a {@link NetworkCloudlet}.
    *
    * //@param vm the VM that will run the created { @link  NetworkCloudlet)
    * @return
    */
  private def createNetworkCloudlet = {
    val netCloudlet = new NetworkCloudlet(cloudletid, 4000, HorizontalVmScalingLB.HOST_PES)
    cloudletid+=1
    netCloudlet.setMemory(HorizontalVmScalingLB.TASK_RAM).setFileSize(HorizontalVmScalingLB.CLOUDLET_FILE_SIZE).setOutputSize(HorizontalVmScalingLB.CLOUDLET_OUTPUT_SIZE).setUtilizationModel(new UtilizationModelFull)
    //netCloudlet.setVm(vm)
    netCloudlet
  }

  /**
    * Adds a send task to the list of tasks of the given {@link NetworkCloudlet}.
    *
    * @param sourceCloudlet the { @link NetworkCloudlet} from which packets will be sent
    * @param destinationCloudlet the destination { @link NetworkCloudlet} to send packets to
    */
  private def addSendTask(sourceCloudlet: NetworkCloudlet, destinationCloudlet: NetworkCloudlet): Unit = {
    val task = new CloudletSendTask(sourceCloudlet.getTasks.size)
    task.setMemory(HorizontalVmScalingLB.TASK_RAM)
    sourceCloudlet.addTask(task)
    val range = 0 until HorizontalVmScalingLB.NUMBER_OF_PACKETS_TO_SEND
   range.foreach(_->
      task.addPacket(destinationCloudlet, HorizontalVmScalingLB.PACKET_DATA_LENGTH_IN_BYTES)
   )
  }

  /**
    * Adds a receive task to the list of tasks of the given
    * {@link NetworkCloudlet}.
    *
    * @param cloudlet the { @link NetworkCloudlet} the task will belong to
    * @param sourceCloudlet the { @link NetworkCloudlet} expected to receive packets from
    */
  private def addReceiveTask(cloudlet: NetworkCloudlet, sourceCloudlet: NetworkCloudlet): Unit = {
    //print("Source cloudlet "+sourceCloudlet.getId+" is associated with VM "+sourceCloudlet.getVm.getId)
    val task = new CloudletReceiveTask(cloudlet.getTasks.size, sourceCloudlet.getVm)
    task.setMemory(HorizontalVmScalingLB.TASK_RAM)
    task.setExpectedPacketsToReceive(HorizontalVmScalingLB.NUMBER_OF_PACKETS_TO_SEND)
    cloudlet.addTask(task)
  }



}

