package com.cloudsimplus.app

import org.cloudbus.cloudsim.allocationpolicies.VmAllocationPolicySimple
import org.cloudbus.cloudsim.brokers.DatacenterBroker
import org.cloudbus.cloudsim.brokers.DatacenterBrokerSimple
import org.cloudbus.cloudsim.cloudlets.network._
import org.cloudbus.cloudsim.core.CloudSim
import org.cloudbus.cloudsim.datacenters.network.NetworkDatacenter
import org.cloudbus.cloudsim.hosts.Host
import org.cloudbus.cloudsim.hosts.network.NetworkHost
import org.cloudbus.cloudsim.network.switches.{AggregateSwitch, EdgeSwitch, RootSwitch}
import org.cloudbus.cloudsim.provisioners.PeProvisionerSimple
import org.cloudbus.cloudsim.provisioners.ResourceProvisionerSimple
import org.cloudbus.cloudsim.resources.Pe
import org.cloudbus.cloudsim.resources.PeSimple
import org.cloudbus.cloudsim.schedulers.cloudlet.{CloudletSchedulerSpaceShared, CloudletSchedulerTimeShared}
import org.cloudbus.cloudsim.schedulers.vm.VmSchedulerTimeShared
import org.cloudbus.cloudsim.utilizationmodels.UtilizationModelFull
import org.cloudbus.cloudsim.vms.network.NetworkVm
import org.cloudsimplus.builders.tables.CloudletsTableBuilderWithCost
import java.util
import java.util.ArrayList

import com.cloudsimplus.app.NetworkAbstract.{TASK_RAM, defaultConfig}
import org.slf4j.{Logger, LoggerFactory}
import com.typesafe.config.{Config, ConfigFactory}
import org.cloudbus.cloudsim.distributions.UniformDistr
import org.cloudsimplus.listeners.EventInfo


/**
  * Abstract class that defines the network architecture
  * Default uses a round robin load balancer for assigning workloads to VM
  */
object NetworkAbstract {


  val logger : Logger = LoggerFactory.getLogger(NetworkAbstract.getClass)

  val defaultConfig: Config = ConfigFactory.parseResources("defaults.conf")
  logger.info("Configuration files loaded")

  val NUMBER_OF_VMS = defaultConfig.getInt("vm.number")
  val VM_PES = defaultConfig.getInt("vm.pes")
  private val VM_RAM = defaultConfig.getInt("vm.ram")
  private val VM_BW = defaultConfig.getInt("vm.bw")
  private val VM_STORAGE = defaultConfig.getInt("vm.storage")
  private val VM_MIPS = defaultConfig.getInt("vm.mips")

  private val NUMBER_OF_HOSTS = defaultConfig.getInt("hosts.number")
  private val HOST_MIPS = defaultConfig.getInt("hosts.mips")
  private val HOST_PES = defaultConfig.getInt("hosts.pes")
  private val HOST_RAM = defaultConfig.getInt("hosts.ram") // host memory (Megabyte)
  private val HOST_STORAGE = defaultConfig.getInt("hosts.storage") // host storage
  private val HOST_BW = defaultConfig.getInt("hosts.bw")

  private val CLOUDLET_FILE_SIZE = defaultConfig.getLong("cloudlet.file_size")
  private val CLOUDLET_OUTPUT_SIZE = defaultConfig.getLong("cloudlet.op_size")
  private val PACKET_DATA_LENGTH_IN_BYTES = defaultConfig.getInt("cloudlet.packet_data_length_in_bytes")
  private val NUMBER_OF_PACKETS_TO_SEND = defaultConfig.getInt("cloudlet.number_of_packets")
  private val TASK_RAM = defaultConfig.getInt("cloudlet.task_ram")
  private val CLOUDLET_PES = defaultConfig.getInt("cloudlet.pes")

  val DYNAMIC_CLOUDLETS_AT_A_TIME = defaultConfig.getInt("cloudlet.number_of_dynamic_cloudlets")
  val INITIAL_CLOUDLETS = defaultConfig.getInt("cloudlet.initial_cloudlets")
  val NUMBER_OF_CLOUDLETS = defaultConfig.getInt("cloudlet.number")

  private val CLOUDLET_LENGTHS = Array(2000, 10000, 30000,16000, 4000, 2000, 20000)
  private val COST_PER_BW = defaultConfig.getDouble("datacenter.cost_per_bw")
  private val COST_PER_SECOND = defaultConfig.getDouble("datacenter.cost_per_sec")


  private val SCHEDULING_INTERVAL = defaultConfig.getInt("simulation.scheduling_interval")
  private val RANDOM_SAMPLE = defaultConfig.getDouble("simulation.random_sample")

}

abstract class NetworkAbstract {

  val logger : Logger = LoggerFactory.getLogger(NetworkAbstract.getClass)

  /**
    * Creates, starts, stops the simulation and shows results.
    */
  logger.info("Starting " + getClass.getSimpleName)
  var cloudletlistsize = 0
  val simulation: CloudSim = new CloudSim
  val TIME_TO_TERMINATE_SIMULATION: Double = defaultConfig.getInt("simulation.time_to_terminate")
  simulation.terminateAt(TIME_TO_TERMINATE_SIMULATION)
  simulation.addOnClockTickListener(createRandomCloudlets)
  val random = new UniformDistr()
  val datacenter: NetworkDatacenter = createDatacenter
  val broker: DatacenterBroker = new DatacenterBrokerSimple(simulation)
  val vmList = createAndSubmitVMs(broker)
  val cloudletList = new util.ArrayList[NetworkCloudlet]
  createNetworkCloudlets
  broker.submitCloudletList(cloudletList)
  simulation.addOnClockTickListener(createRandomCloudlets)
  simulation.start
  showSimulationResults()

  /**
    * Get total cost of the simulation
   */
  private def showTotalCost(): Unit = {
    logger.info("Calculate total cost")

    var totalCost = 0.0D
    var meantime = 0.0D
    val newList = broker.getCloudletFinishedList
    cloudletList.forEach(c => {
      totalCost = totalCost + c.getTotalCost
      meantime = meantime + c.getActualCpuTime
    })
    meantime = meantime/cloudletList.size()
    println("Total cost of execution of " + newList.size + " Cloudlets = $" + Math.round(totalCost * 100D)/100D)
    println("mean cost of execution of " + newList.size + " Cloudlets = $" + (totalCost/newList.size()))

    println("Total mean time of actualCPUTime for " + newList.size + " Cloudlets = " + meantime)

  }

  /**
    * Print Simulation results
    */
  private def showSimulationResults(): Unit = {
    logger.info("Print Simulation results")

    val newList = broker.getCloudletFinishedList
    new CloudletsTableBuilderWithCost(newList).build()
    println("Number of Actual cloudlets = "+NetworkAbstract.INITIAL_CLOUDLETS+"; dynamic cloudlets = "+(cloudletList.size()-NetworkAbstract.INITIAL_CLOUDLETS))

    import scala.collection.JavaConversions._
    for (host <- datacenter.getHostList[NetworkHost]) {
      println("Host " + host.getId + " data transferred: " + host.getTotalDataTransferBytes + " bytes")
    }

    showTotalCost()

    println(this.getClass.getSimpleName + " finished!")
  }

  /**
    * Creates the Datacenter.
    *
    * @return the Datacenter
    */
  private def createDatacenter = {
    logger.info("Creating a datacenter")

    val numberOfHosts = NetworkAbstract.NUMBER_OF_HOSTS * AggregateSwitch.PORTS * RootSwitch.PORTS

    val hostList = new util.ArrayList[Host]
    val range = 0 until numberOfHosts
    range.foreach { _ =>
      val host = createHost
      hostList.add(host)
    }
    val newDatacenter = new NetworkDatacenter(simulation, hostList, new VmAllocationPolicySimple)
    newDatacenter.setSchedulingInterval(NetworkAbstract.SCHEDULING_INTERVAL)
    newDatacenter.getCharacteristics.setCostPerBw(NetworkAbstract.COST_PER_BW)
    newDatacenter.getCharacteristics.setCostPerSecond(NetworkAbstract.COST_PER_SECOND)
    createNetwork(newDatacenter)
    newDatacenter
  }

  /**
    * Creates the Host.
    *
    * @return the NetworkHost
    */

  private def createHost = {
    logger.info("Creating a new NetworkHost")

    val peList = createPEs(NetworkAbstract.HOST_PES, NetworkAbstract.HOST_MIPS)
    new NetworkHost(NetworkAbstract.HOST_RAM, NetworkAbstract.HOST_BW, NetworkAbstract.HOST_STORAGE, peList).setRamProvisioner(new ResourceProvisionerSimple).setBwProvisioner(new ResourceProvisionerSimple).setVmScheduler(new VmSchedulerTimeShared)
  }

  /**
    * Creates the list of PE with mips as specified
    * @param numberOfPEs size of the list
    * @param mips value of mips for each PE in the list
    * @return the Datacenter
    */

  private def createPEs(numberOfPEs: Int, mips: Long) = {
    logger.info("Creating "+numberOfPEs+" PEs with mips "+mips)

    val peList = new util.ArrayList[Pe]
    val range = 0 until numberOfPEs
    range.foreach { _ =>
      peList.add(new PeSimple(mips, new PeProvisionerSimple))
    }
    peList
  }

  /**
    * Creates internal Datacenter network.
    *
    * @param datacenter Datacenter where the network will be created
    */
  private def createNetwork(datacenter: NetworkDatacenter): Unit = {
    logger.info("Creating internal network for "+datacenter)

    val numberOfEdgeSwitches = NetworkAbstract.NUMBER_OF_HOSTS - 1
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

  /**
    * Creates a list of virtual machines in a Datacenter for a given broker and
    * submit the list to the broker.
    *
    * @param broker The broker that will own the created VMs
    * @return the list of created VMs
    */
  private def createAndSubmitVMs(broker: DatacenterBroker) = {
    logger.info("Creating and submit vms to broker "+broker)
    val list = new util.ArrayList[NetworkVm]
    val range = 0 until NetworkAbstract.NUMBER_OF_VMS
    range.foreach { hostId =>
      val vm = createVm(hostId)
      list.add(vm)
    }
    broker.submitVmList(list)
    list
  }

  /**
    * Creates a virtual machine with the given id
    *
    * @param id The VM id
    * @return instance of NetworkVM
    */

  private def createVm(id: Int) = {
    logger.info("Creating NetworkVM with id "+id)
    val vm = new NetworkVm(id, NetworkAbstract.VM_MIPS, NetworkAbstract.VM_PES)
    vm.setRam(NetworkAbstract.VM_RAM).setBw(NetworkAbstract.VM_BW).setSize(NetworkAbstract.VM_STORAGE).setCloudletScheduler(new CloudletSchedulerSpaceShared)
    vm
  }

  /**
    * Creates a list of {@link NetworkCloudlet} that together represents the
    * distributed processes of a given fictitious application.
    * Must be implemented my the implementing class
    * @return the list of create NetworkCloudlets
    */
  protected def createNetworkCloudlets: util.ArrayList[NetworkCloudlet]



  /**
    * Creates execution, send and receive tasks for cloudlets
    * @param networkCloudletList <ArrayList> cloudlets
    *
    */

  protected def createTasksForNetworkCloudlets(networkCloudletList: util.ArrayList[NetworkCloudlet]): Unit = {
    logger.info("Creating tasks for network cloudlets")
    import scala.collection.JavaConversions._
    for (cloudlet <- networkCloudletList) {
      addExecutionTask(cloudlet)
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
    * Creates a {@link NetworkCloudlet}.
    *
    * @param vm the VM that will run the created { @link  NetworkCloudlet)
    * @return
    */
  protected def createNetworkCloudlet(vm: NetworkVm) = {
    val id = cloudletList.size()
    logger.info("Creating network cloudlet")
    val rand = cloudletList.size() % NetworkAbstract.CLOUDLET_LENGTHS.size
    val length = NetworkAbstract.CLOUDLET_LENGTHS(rand)
    val netCloudlet = new NetworkCloudlet(id, length, NetworkAbstract.CLOUDLET_PES)
    netCloudlet.setMemory(NetworkAbstract.TASK_RAM).setFileSize(NetworkAbstract.CLOUDLET_FILE_SIZE).setOutputSize(NetworkAbstract.CLOUDLET_OUTPUT_SIZE).setUtilizationModel(new UtilizationModelFull)
    netCloudlet.setVm(vm)
    netCloudlet
  }

  /**
    * Adds a send task to the list of tasks of the given {@link NetworkCloudlet}.
    *
    * @param sourceCloudlet the { @link NetworkCloudlet} from which packets will be sent
    * @param destinationCloudlet the destination { @link NetworkCloudlet} to send packets to
    */
  private def addSendTask(sourceCloudlet: NetworkCloudlet, destinationCloudlet: NetworkCloudlet): Unit = {
    logger.info("Adding send task from cloudlet "+sourceCloudlet.getId+" to cloudlet "+destinationCloudlet.getId)

    val task = new CloudletSendTask(sourceCloudlet.getTasks.size)
    task.setMemory(NetworkAbstract.TASK_RAM)
    sourceCloudlet.addTask(task)
    val range = 0 until NetworkAbstract.NUMBER_OF_PACKETS_TO_SEND
    range.foreach { _ =>
      task.addPacket(destinationCloudlet, NetworkAbstract.PACKET_DATA_LENGTH_IN_BYTES)
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
    logger.info("Adding receiving task from cloudlet "+sourceCloudlet.getId+" to cloudlet "+cloudlet.getId)
    val task = new CloudletReceiveTask(cloudlet.getTasks.size, sourceCloudlet.getVm)
    task.setMemory(NetworkAbstract.TASK_RAM)
    task.setExpectedPacketsToReceive(NetworkAbstract.NUMBER_OF_PACKETS_TO_SEND)
    cloudlet.addTask(task)
  }

  /**
    * Adds an execution task to the list of tasks of the given
    * {@link NetworkCloudlet}.
    *
    * @param cloudlet the { @link NetworkCloudlet} the task will belong to
    */
  private def addExecutionTask(cloudlet: NetworkCloudlet): Unit = {
    logger.info("Adding execution task for cloudlet "+cloudlet.getId)
    val task = new CloudletExecutionTask(cloudlet.getTasks.size, (cloudlet.getTotalLength/2))
    task.setMemory(TASK_RAM)
    cloudlet.addTask(task)
  }

  /**
    * Simulates the dynamic arrival of Cloudlets, randomly during simulation runtime.
    * At any time the simulation clock updates or the number of cloudlets created is less than specified,
    * a new Cloudlet will be created with a probability specified in RANDOM_SAMPLE.
    *
    * @param evt
    */
  private def createRandomCloudlets(evt: EventInfo): Unit = {
    if (random.sample() <= NetworkAbstract.RANDOM_SAMPLE && cloudletlistsize != NetworkAbstract.NUMBER_OF_CLOUDLETS) {
      logger.info("\n#Randomly creating more cloudlets at time "+ evt.getTime+"\n")
      broker.submitCloudletList(createNetworkCloudlets)
    }
  }
}
