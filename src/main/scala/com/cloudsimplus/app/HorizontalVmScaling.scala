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
import org.cloudbus.cloudsim.schedulers.cloudlet.CloudletSchedulerTimeShared
import org.cloudbus.cloudsim.schedulers.vm.VmSchedulerTimeShared
import org.cloudbus.cloudsim.utilizationmodels.UtilizationModelFull
import org.cloudbus.cloudsim.vms.network.NetworkVm
import org.cloudsimplus.builders.tables.CloudletsTableBuilder
import java.util
import java.util.ArrayList

import com.cloudsimplus.app.LoadBalancerByHorizontalVmScaling.SCHEDULING_INTERVAL
import org.cloudbus.cloudsim.distributions.UniformDistr
import org.cloudbus.cloudsim.vms.Vm
import org.cloudsimplus.app.NetworkLB.{addExecutionTask, addReceiveTask, addSendTask}
import org.cloudsimplus.autoscaling.HorizontalVmScalingSimple
import org.cloudsimplus.listeners.EventInfo


/**
  * A simple example simulating a distributed application.
  * It show how 2 {@link NetworkCloudlet}'s communicate,
  * each one running inside VMs on different hosts.
  *
  * @author Manoel Campos da Silva Filho
  */
object HorizontalVmScaling {
  private val NUMBER_OF_HOSTS = 50
  private val HOST_MIPS = 1000
  private val HOST_PES = 2
  private val HOST_RAM = 2048 // host memory (Megabyte)

  private val HOST_STORAGE = 1000000 // host storage

  private val HOST_BW = 10000
  private val CLOUDLET_EXECUTION_TASK_LENGTH = 200
  private val CLOUDLET_FILE_SIZE = 300
  private val CLOUDLET_OUTPUT_SIZE = 300
  private val PACKET_DATA_LENGTH_IN_BYTES = 1000
  private val NUMBER_OF_PACKETS_TO_SEND = 1
  private val TASK_RAM = 1000

  private val VMS = 4
  private val CLOUDLETS = 2

  private val SCHEDULING_INTERVAL = 5
  private val CLOUDLETS_CREATION_INTERVAL = SCHEDULING_INTERVAL * 2





  /**
    * Starts the execution of the example.
    *
    * @param args command line arguments
    */
  def main(args: Array[String]): Unit = {
    new HorizontalVmScaling
  }

  /**
    * Adds an execution task to the list of tasks of the given
    * {@link NetworkCloudlet}.
    *
    * @param cloudlet the { @link NetworkCloudlet} the task will belong to
    */
  private def addExecutionTask(cloudlet: NetworkCloudlet): Unit = {
    print("Executing cloudlet "+cloudlet.getId)
    val task = new CloudletExecutionTask(cloudlet.getTasks.size, CLOUDLET_EXECUTION_TASK_LENGTH)
    task.setMemory(TASK_RAM)
    cloudlet.addTask(task)
  }
}

class HorizontalVmScaling private() {

  /**
    * Creates, starts, stops the simulation and shows results.
    */
  println("Starting " + getClass.getSimpleName)


  var cloudletlistsize =0
  var createVms = 0
  private val TIME_TO_TERMINATE_SIMULATION: Double = 1000


  val simulation: CloudSim = new CloudSim
  simulation.terminateAt(TIME_TO_TERMINATE_SIMULATION)
  simulation.addOnClockTickListener(createNetworkCloudlets)
  private var random = new UniformDistr()
  val datacenter: NetworkDatacenter = createDatacenter
  val broker: DatacenterBroker = new DatacenterBrokerSimple(simulation)
  broker.setVmDestructionDelayFunction((vm: Vm) => 1000.0)
  val vmList = new util.ArrayList[NetworkVm](HorizontalVmScaling.VMS)
  vmList.addAll(createListOfScalableVms(HorizontalVmScaling.VMS))
  val cloudletList = new util.ArrayList[NetworkCloudlet]()
  createInitialCloudlets
  broker.submitVmList(vmList)
  broker.submitCloudletList(cloudletList)
  //simulation.addOnClockTickListener(createRandomCloudlets)
  simulation.start
  createNetworkTask
  //createTasksForNetworkCloudlets(cloudletList)
  Thread.sleep(500)
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
      println(s"\nHost %d data transferred: %d bytes", host.getId, host.getTotalDataTransferBytes)
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
    var i = 0
    while ( {
      i < HorizontalVmScaling.NUMBER_OF_HOSTS
    }) {
      val host = createHost
      hostList.add(host)

      {
        i += 1; i - 1
      }
    }
    val newDatacenter = new NetworkDatacenter(simulation, hostList, new VmAllocationPolicySimple)
    newDatacenter.setSchedulingInterval(HorizontalVmScaling.SCHEDULING_INTERVAL)
    //createNetwork(newDatacenter)
    newDatacenter
  }

  private def createHost = {
    val peList = createPEs(HorizontalVmScaling.HOST_PES, HorizontalVmScaling.HOST_MIPS)
    new NetworkHost(HorizontalVmScaling.HOST_RAM, HorizontalVmScaling.HOST_BW, HorizontalVmScaling.HOST_STORAGE, peList).setRamProvisioner(new ResourceProvisionerSimple).setBwProvisioner(new ResourceProvisionerSimple).setVmScheduler(new VmSchedulerTimeShared)
  }

  private def createPEs(numberOfPEs: Int, mips: Long) = {
    val peList = new util.ArrayList[Pe]
    var i = 0
    while ( {
      i < numberOfPEs
    }) {
      peList.add(new PeSimple(mips, new PeProvisionerSimple))

      {
        i += 1; i - 1
      }
    }
    peList
  }

  /**
    * Creates internal Datacenter network.
    *
    * @param datacenter Datacenter where the network will be created
    */
  private def createNetwork(datacenter: NetworkDatacenter): Unit = {

    val numberOfEdgeSwitches = HorizontalVmScaling.NUMBER_OF_HOSTS-1
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
    val vm = new NetworkVm(id, HorizontalVmScaling.HOST_MIPS, HorizontalVmScaling.HOST_PES)
    vm.setRam(HorizontalVmScaling.HOST_RAM).setBw(HorizontalVmScaling.HOST_BW).setSize(HorizontalVmScaling.HOST_STORAGE).setCloudletScheduler(new CloudletSchedulerTimeShared)
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
    val numberOfCloudlets = HorizontalVmScaling.CLOUDLETS
    val networkCloudletList = new util.ArrayList[NetworkCloudlet](numberOfCloudlets)
    val range = 0 until HorizontalVmScaling.CLOUDLETS
    range.foreach{_=>
      print("in range")
      cloudletList.add(createNetworkCloudlet)
    }

  }

  private def createNetworkTask ={
    //NetworkCloudlet 0 Tasks
    HorizontalVmScaling.addExecutionTask(cloudletList.get(0))
    addSendTask(cloudletList.get(0), cloudletList.get(1))
    //NetworkCloudlet 1 Tasks
    addReceiveTask(cloudletList.get(1), cloudletList.get(0))
    HorizontalVmScaling.addExecutionTask(cloudletList.get(1))
  }

  private def createTasksForNetworkCloudlets(networkCloudletList: util.ArrayList[NetworkCloudlet]): Unit = {
    import scala.collection.JavaConversions._
    for (cloudlet <- networkCloudletList) {
      HorizontalVmScaling.addExecutionTask(cloudlet)
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
    print("Creating new cloudlets")
    val time = eventInfo.getTime.toLong
    if (time % HorizontalVmScaling.CLOUDLETS_CREATION_INTERVAL == 0 && time <= 500) {
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
      broker.submitCloudletList(cloudletList)
      //createTasksForNetworkCloudlets(cloudletList)
      createNetworkTask
    }

  }


  /**
    * Creates a {@link NetworkCloudlet}.
    *
    * @param vm the VM that will run the created { @link  NetworkCloudlet)
    * @return
    */
  private def createNetworkCloudlet = {
    val netCloudlet = new NetworkCloudlet(4000, HorizontalVmScaling.HOST_PES)
    netCloudlet.setMemory(HorizontalVmScaling.TASK_RAM).setFileSize(HorizontalVmScaling.CLOUDLET_FILE_SIZE).setOutputSize(HorizontalVmScaling.CLOUDLET_OUTPUT_SIZE).setUtilizationModel(new UtilizationModelFull)
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
    task.setMemory(HorizontalVmScaling.TASK_RAM)
    sourceCloudlet.addTask(task)
    var i = 0
    while ( {
      i < HorizontalVmScaling.NUMBER_OF_PACKETS_TO_SEND
    }) {
      task.addPacket(destinationCloudlet, HorizontalVmScaling.PACKET_DATA_LENGTH_IN_BYTES)

      {
        i += 1; i - 1
      }
    }
  }

  /**
    * Adds a receive task to the list of tasks of the given
    * {@link NetworkCloudlet}.
    *
    * @param cloudlet the { @link NetworkCloudlet} the task will belong to
    * @param sourceCloudlet the { @link NetworkCloudlet} expected to receive packets from
    */
  private def addReceiveTask(cloudlet: NetworkCloudlet, sourceCloudlet: NetworkCloudlet): Unit = {
    val task = new CloudletReceiveTask(cloudlet.getTasks.size, sourceCloudlet.getVm)
    task.setMemory(HorizontalVmScaling.TASK_RAM)
    task.setExpectedPacketsToReceive(HorizontalVmScaling.NUMBER_OF_PACKETS_TO_SEND)
    cloudlet.addTask(task)
  }


}

