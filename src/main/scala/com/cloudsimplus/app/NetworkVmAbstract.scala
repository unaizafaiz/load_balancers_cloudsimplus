package org.cloudbus.cloudsim.examples.network.applications

import org.cloudbus.cloudsim.allocationpolicies.VmAllocationPolicySimple
import org.cloudbus.cloudsim.brokers.DatacenterBroker
import org.cloudbus.cloudsim.brokers.DatacenterBrokerSimple
import org.cloudbus.cloudsim.cloudlets.Cloudlet
import org.cloudbus.cloudsim.cloudlets.network._
import org.cloudbus.cloudsim.core.CloudSim
import org.cloudbus.cloudsim.datacenters.network.NetworkDatacenter
import org.cloudbus.cloudsim.distributions.UniformDistr
import org.cloudbus.cloudsim.hosts.Host
import org.cloudbus.cloudsim.hosts.network.NetworkHost
import org.cloudbus.cloudsim.network.switches.AggregateSwitch
import org.cloudbus.cloudsim.network.switches.EdgeSwitch
import org.cloudbus.cloudsim.network.switches.RootSwitch
import org.cloudbus.cloudsim.provisioners.PeProvisionerSimple
import org.cloudbus.cloudsim.provisioners.ResourceProvisionerSimple
import org.cloudbus.cloudsim.resources.Pe
import org.cloudbus.cloudsim.resources.PeSimple
import org.cloudbus.cloudsim.schedulers.cloudlet.CloudletSchedulerTimeShared
import org.cloudbus.cloudsim.schedulers.vm.VmSchedulerTimeShared
import org.cloudbus.cloudsim.vms.network.NetworkVm
import org.cloudsimplus.builders.tables.CloudletsTableBuilder
import java.util
import java.util.{ArrayList, HashMap, List, Map}

import scala.collection.mutable.ArrayBuffer


/**
  * A base class for network simulation examples
  * using objects such as{@link NetworkDatacenter},
  * {@link NetworkHost}, {@link NetworkVm} and {@link NetworkCloudlet}.
  *
  * The class simulate applications that are compounded by a list of
  * {@link NetworkCloudlet}.
  *
  * @author Saurabh Kumar Garg
  * @author Rajkumar Buyya
  * @author Manoel Campos da Silva Filho
  */
object NetworkVmExampleAbstract {
  val MAX_VMS_PER_HOST = 2
  val COST = 3.0 // the cost of using processing in this resource

  val COST_PER_MEM = 0.05 // the cost of using memory in this resource

  val COST_PER_STORAGE = 0.001 // the cost of using storage in this resource

  val COST_PER_BW = 0.0 // the cost of using bw in this resource

  val HOST_MIPS = 1000
  val HOST_PES = 8
  val HOST_RAM = 2048 // MEGA

  val HOST_STORAGE = 1000000
  val HOST_BW = 10000
  val VM_MIPS = 1000
  val VM_SIZE = 10000 // image size (Megabyte)

  val VM_RAM = 512
  val VM_BW = 1000
  val VM_PES_NUMBER: Int = HOST_PES / MAX_VMS_PER_HOST
  /**
    * Number of fictitious applications to create.
    * Each application is just a list of {@link NetworkCloudlet}.
    *
    * @see #appMap
    */
  val NUMBER_OF_APPS = 1
  val NETCLOUDLET_PES_NUMBER: Int = VM_PES_NUMBER
  val NETCLOUDLET_EXECUTION_TASK_LENGTH = 4000
  val NETCLOUDLET_FILE_SIZE = 300
  val NETCLOUDLET_OUTPUT_SIZE = 300
  val NETCLOUDLET_RAM = 100
  private val PACKET_DATA_LENGTH_IN_BYTES = 1000
  private val NUMBER_OF_PACKETS_TO_SEND = 100
  private val SCHEDULING_INTERVAL = 5

  /**
    * Adds an execution task to the list of tasks of the given {@link NetworkCloudlet}.
    *
    * @param cloudlet the { @link NetworkCloudlet} the task will belong to
    */
  protected def addExecutionTask(cloudlet: NetworkCloudlet): Unit = {
    val task = new CloudletExecutionTask(cloudlet.getTasks.size, NETCLOUDLET_EXECUTION_TASK_LENGTH)
    task.setMemory(NETCLOUDLET_RAM)
    cloudlet.addTask(task)
  }

  /**
    * Gets the index of a switch where a Host will be connected,
    * considering the number of ports the switches have.
    * Ensures that each set of N Hosts is connected to the same switch
    * (where N is defined as the number of switch's ports).
    * Since the host ID is long but the switch array index is int,
    * the module operation is used safely convert a long to int
    * For instance, if the id is 2147483648 (higher than the max int value 2147483647),
    * it will be returned 0. For 2147483649 it will be 1 and so on.
    *
    * @param host        the Host to get the index of the switch to connect to
    * @param switchPorts the number of ports (N) the switches where the Host will be connected have
    * @return the index of the switch to connect the host
    */
  def getSwitchIndex(host: NetworkHost, switchPorts: Int): Long = host.getId % Integer.MAX_VALUE.round / switchPorts
}

abstract class NetworkVmExampleAbstract private[applications]() {

/**
  * Creates, starts, stops the simulation and shows results.
  */
  /*Enables just some level of log messages.
           Make sure to import org.cloudsimplus.util.Log;*/
  //Log.setLevel(ch.qos.logback.classic.Level.WARN);
  println("Starting " + getClass.getSimpleName)
  val simulation: CloudSim = new CloudSim

  val datacenter: NetworkDatacenter = createDatacenter
  val brokerList = createBrokerForEachApp
  val vmList = new ArrayList[NetworkVm]
  val appMap = new HashMap[Integer, List[NetworkCloudlet]]
  var appId: Int = -1

  import scala.collection.JavaConversions._

  for (broker <- brokerList) {
    vmList.addAll(createAndSubmitVMs(broker))
    appMap.put({appId += 1; appId}, createAppAndSubmitToBroker(broker))
  }
  simulation.start
  showSimulationResults()

  /**
    * @see #getVmList()
    */

  //private var brokerList = null
  /**
    * A Map representing a list of cloudlets from different applications.
    * Each key represents the ID of a fictitious app that is composed of a list
    * of {@link NetworkCloudlet}.
    */

  private def showSimulationResults(): Unit = {
    var i = 0
    while ( {
      i < NetworkVmExampleAbstract.NUMBER_OF_APPS
    }) {
      var broker = brokerList.get(i)
      val newList = broker.getCloudletFinishedList
      val caption = broker.getName + " - Application " + broker.getId
      new CloudletsTableBuilder(newList).setTitle(caption).build()
      println("Number of NetworkCloudlets for Application %s: %d\n", broker.getId, newList.size)

      {
        i += 1; i - 1
      }
    }
    import scala.collection.JavaConversions._
    for (host <- datacenter.getHostList[NetworkHost]) {
      println("\nHost %d data transferred: %d bytes", host.getId, host.getTotalDataTransferBytes)
    }
    println(this.getClass.getSimpleName + " finished!")
  }

  /**
    * Create a {@link DatacenterBroker} for each each list of {@link NetworkCloudlet},
    * representing cloudlets that compose the same application.
    *
    * @return the list of created NetworkDatacenterBroker
    */
  private def createBrokerForEachApp = {
    val list = new util.ArrayList[DatacenterBroker]
    var i = 0
    while ( {
      i < NetworkVmExampleAbstract.NUMBER_OF_APPS
    }) {
      list.add(new DatacenterBrokerSimple(simulation))

      {
        i += 1; i - 1
      }
    }
    list
  }

  /**
    * Creates the Datacenter.
    *
    * @return the Datacenter
    */
  final protected def createDatacenter: NetworkDatacenter = {
    val numberOfHosts = EdgeSwitch.PORTS * AggregateSwitch.PORTS * RootSwitch.PORTS
    val hostList = new util.ArrayList[Host](numberOfHosts)
    var i = 0
    while ( {
      i < numberOfHosts
    }) {
      val peList = createPEs(NetworkVmExampleAbstract.HOST_PES, NetworkVmExampleAbstract.HOST_MIPS)
      val host = new NetworkHost(NetworkVmExampleAbstract.HOST_RAM, NetworkVmExampleAbstract.HOST_BW, NetworkVmExampleAbstract.HOST_STORAGE, peList).setRamProvisioner(new ResourceProvisionerSimple).setBwProvisioner(new ResourceProvisionerSimple).setVmScheduler(new VmSchedulerTimeShared)
      hostList.add(host)

      {
        i += 1; i - 1
      }
    }
    val dc = new NetworkDatacenter(simulation, hostList, new VmAllocationPolicySimple)
    dc.setSchedulingInterval(NetworkVmExampleAbstract.SCHEDULING_INTERVAL)
    dc.getCharacteristics.setCostPerSecond(NetworkVmExampleAbstract.COST).setCostPerMem(NetworkVmExampleAbstract.COST_PER_MEM).setCostPerStorage(NetworkVmExampleAbstract.COST_PER_STORAGE).setCostPerBw(NetworkVmExampleAbstract.COST_PER_BW)
    createNetwork(dc)
    dc
  }

  protected def createPEs(pesNumber: Int, mips: Long): util.List[Pe] = {
    val peList = new util.ArrayList[Pe]
    var i = 0
    while ( {
      i < pesNumber
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
  protected def createNetwork(datacenter: NetworkDatacenter): Unit = {
    val edgeSwitches: ArrayBuffer[EdgeSwitch] = new ArrayBuffer[EdgeSwitch](1)

    edgeSwitches.foreach((edgeSwitch) => {
      edgeSwitches.add(new EdgeSwitch(simulation, datacenter))
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
    * Creates a list of virtual machines in a Datacenter for a given broker
    * and submit the list to the broker.
    *
    * @param broker The broker that will own the created VMs
    * @return the list of created VMs
    */
  final protected def createAndSubmitVMs(broker: DatacenterBroker): util.List[NetworkVm] = {
    val numberOfVms = getDatacenterHostList.size * NetworkVmExampleAbstract.MAX_VMS_PER_HOST
    val list = new util.ArrayList[NetworkVm]
    var i = 0
    while ( {
      i < numberOfVms
    }) {
      val vm = new NetworkVm(i, NetworkVmExampleAbstract.VM_MIPS, NetworkVmExampleAbstract.VM_PES_NUMBER)
      vm.setRam(NetworkVmExampleAbstract.VM_RAM).setBw(NetworkVmExampleAbstract.VM_BW).setSize(NetworkVmExampleAbstract.VM_SIZE).setCloudletScheduler(new CloudletSchedulerTimeShared)
      list.add(vm)

      {
        i += 1; i - 1
      }
    }
    broker.submitVmList(list)
    list
  }

  private def getDatacenterHostList = datacenter.getHostList

  /**
    * Randomly select a given number of VMs from the list of created VMs,
    * to be used by the NetworkCloudlets of the given application.
    *
    * @param broker              the broker where to get the existing VM list
    * @param numberOfVmsToSelect number of VMs to selected from the existing list of VMs.
    * @return The list of randomly selected VMs
    */
  protected def randomlySelectVmsForApp(broker: DatacenterBroker, numberOfVmsToSelect: Int): util.List[NetworkVm] = {
    val list = new util.ArrayList[NetworkVm]
    val numOfExistingVms = this.vmList.size
    val rand = new UniformDistr(0, numOfExistingVms, 5)
    var i = 0
    while ( {
      i < numberOfVmsToSelect
    }) {
      val vmIndex = rand.sample.toInt % vmList.size
      val vm = vmList.get(vmIndex)
      list.add(vm)

      {
        i += 1; i - 1
      }
    }
    list
  }

  /**
    * @return List of VMs of all Brokers.
    */
  def getVmList: util.List[NetworkVm] = vmList

  def getDatacenter: NetworkDatacenter = datacenter

  def getBrokerList: util.List[DatacenterBroker] = brokerList

  /**
    * Creates a list of {@link NetworkCloudlet}'s that represents
    * a single application and then submits the created cloudlets to a given Broker.
    *
    * @param broker the broker to submit the list of NetworkCloudlets of the application
    * @return the list of created  { @link NetworkCloudlet}'s
    */
  final protected def createAppAndSubmitToBroker(broker: DatacenterBroker): util.List[NetworkCloudlet] = {
    val list = createNetworkCloudlets(broker)
    broker.submitCloudletList(list)
    list
  }

  /**
    * Creates a list of {@link NetworkCloudlet} that together represents the distributed
    * processes of a given application.
    *
    * @param broker broker to associate the NetworkCloudlets
    * @return the list of create NetworkCloudlets
    */
  protected def createNetworkCloudlets(broker: DatacenterBroker): util.List[NetworkCloudlet]

  /**
    * Adds a send task to the list of tasks of the given {@link NetworkCloudlet}.
    *
    * @param sourceCloudlet the { @link NetworkCloudlet} from which packets will be sent
    * @param destinationCloudlet the destination { @link NetworkCloudlet} to send packets to
    */
  protected def addSendTask(sourceCloudlet: NetworkCloudlet, destinationCloudlet: NetworkCloudlet): Unit = {
    val task = new CloudletSendTask(sourceCloudlet.getTasks.size)
    task.setMemory(NetworkVmExampleAbstract.NETCLOUDLET_RAM)
    sourceCloudlet.addTask(task)
    var i = 0
    while ( {
      i < NetworkVmExampleAbstract.NUMBER_OF_PACKETS_TO_SEND
    }) {
      task.addPacket(destinationCloudlet, NetworkVmExampleAbstract.PACKET_DATA_LENGTH_IN_BYTES)

      {
        i += 1; i - 1
      }
    }
  }

  /**
    * Adds a receive task to the list of tasks of the given {@link NetworkCloudlet}.
    *
    * @param cloudlet the { @link NetworkCloudlet} the task will belong to
    * @param sourceCloudlet the { @link NetworkCloudlet} expected to receive packets from
    */
  protected def addReceiveTask(cloudlet: NetworkCloudlet, sourceCloudlet: NetworkCloudlet): Unit = {
    val task = new CloudletReceiveTask(cloudlet.getTasks.size, sourceCloudlet.getVm)
    task.setMemory(NetworkVmExampleAbstract.NETCLOUDLET_RAM)
    task.setExpectedPacketsToReceive(NetworkVmExampleAbstract.NUMBER_OF_PACKETS_TO_SEND)
    cloudlet.addTask(task)
  }
}
