package com.cloudsimplus.app

import org.cloudbus.cloudsim.allocationpolicies.VmAllocationPolicySimple
import org.cloudbus.cloudsim.brokers.DatacenterBroker
import org.cloudbus.cloudsim.brokers.DatacenterBrokerSimple
import org.cloudbus.cloudsim.cloudlets.Cloudlet
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
import org.cloudsimplus.builders.tables.{CloudletsTableBuilder, CloudletsTableBuilderWithCost}
import java.util
import java.util.ArrayList

import org.slf4j.{Logger, LoggerFactory}
import com.typesafe.config.{Config, ConfigFactory}
import org.cloudbus.cloudsim.distributions.UniformDistr
import org.cloudsimplus.listeners.EventInfo


/**
  * Load balancing by round robin order of VM assignment
  */
object RoundRobinLoadBalancer {

  val logger : Logger = LoggerFactory.getLogger(RoundRobinLoadBalancer.getClass)

  val defaultConfig: Config = ConfigFactory.parseResources("defaults.conf")
  logger.info("Configuration files loaded")

  private val NUMBER_OF_HOSTS = defaultConfig.getInt("hosts.number")
  private val HOST_MIPS = 1000
  private val HOST_PES = 4
  private val HOST_RAM = 2048 // host memory (Megabyte)

  private val HOST_STORAGE = 1000000 // host storage

  private val HOST_BW = 10000
  private val CLOUDLET_EXECUTION_TASK_LENGTH = 4000
  private val CLOUDLET_FILE_SIZE = defaultConfig.getLong("cloudlet.file_size")
  private val CLOUDLET_OUTPUT_SIZE = defaultConfig.getLong("cloudlet.op_size")
  private val PACKET_DATA_LENGTH_IN_BYTES = 1000
  private val NUMBER_OF_PACKETS_TO_SEND = 1
  private val TASK_RAM = 100

  private val COST_PER_BW = defaultConfig.getDouble("datacenter.cost_per_bw")
  private val COST_PER_SECOND = defaultConfig.getDouble("datacenter.cost_per_sec")

  /**
    * Starts the execution of the example.
    *
    * @param args command line arguments
    */
  def main(args: Array[String]): Unit = {
    new RoundRobinLoadBalancer
  }

  /**
    * Adds an execution task to the list of tasks of the given
    * {@link NetworkCloudlet}.
    *
    * @param cloudlet the { @link NetworkCloudlet} the task will belong to
    */
  private def addExecutionTask(cloudlet: NetworkCloudlet): Unit = {
    val task = new CloudletExecutionTask(cloudlet.getTasks.size, CLOUDLET_EXECUTION_TASK_LENGTH)
    task.setMemory(TASK_RAM)
    cloudlet.addTask(task)
  }
}

class RoundRobinLoadBalancer private() {

/**
  * Creates, starts, stops the simulation and shows results.
  */
  println("Starting " + getClass.getSimpleName)
  var cloudletlistsize = 0
  val simulation: CloudSim = new CloudSim
  private val TIME_TO_TERMINATE_SIMULATION: Double = 30
  simulation.terminateAt(TIME_TO_TERMINATE_SIMULATION)
  simulation.addOnClockTickListener(createRandomCloudlets)
  private var random = new UniformDistr()
  val datacenter: NetworkDatacenter = createDatacenter
  val broker: DatacenterBroker = new DatacenterBrokerSimple(simulation)
  val vmList = createAndSubmitVMs(broker)
  val cloudletList = createNetworkCloudlets
  broker.submitCloudletList(cloudletList)
  simulation.addOnClockTickListener(createRandomCloudlets)
  simulation.start
  showSimulationResults()

  private def showTotalCost(): Unit = {
    var totalCost = 0.0D
    val newList = broker.getCloudletFinishedList
    cloudletList.forEach(c =>
      totalCost = totalCost + c.getTotalCost
    )
    println("Total cost of execution of " + newList.size + " Cloudlets = $" + Math.round(totalCost * 100D)/100D)
  }

  private def showSimulationResults(): Unit = {
    val newList = broker.getCloudletFinishedList
    new CloudletsTableBuilderWithCost(newList).build()
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
    val hostList = new util.ArrayList[Host]
    var i = 0
    while ( {
      i < RoundRobinLoadBalancer.NUMBER_OF_HOSTS
    }) {
      val host = createHost
      hostList.add(host)

      {
        i += 1; i - 1
      }
    }
    val newDatacenter = new NetworkDatacenter(simulation, hostList, new VmAllocationPolicySimple)
    newDatacenter.setSchedulingInterval(5)
    newDatacenter.getCharacteristics.setCostPerBw(RoundRobinLoadBalancer.COST_PER_BW)
    newDatacenter.getCharacteristics.setCostPerSecond(RoundRobinLoadBalancer.COST_PER_SECOND)
    createNetwork(newDatacenter)
    newDatacenter
  }

  private def createHost = {
    val peList = createPEs(RoundRobinLoadBalancer.HOST_PES, RoundRobinLoadBalancer.HOST_MIPS)
    new NetworkHost(RoundRobinLoadBalancer.HOST_RAM, RoundRobinLoadBalancer.HOST_BW, RoundRobinLoadBalancer.HOST_STORAGE, peList).setRamProvisioner(new ResourceProvisionerSimple).setBwProvisioner(new ResourceProvisionerSimple).setVmScheduler(new VmSchedulerTimeShared)
  }

  private def createPEs(numberOfPEs: Int, mips: Long) = {
    val peList = new util.ArrayList[Pe]
    var i = 0
    while ({
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

    val numberOfEdgeSwitches = RoundRobinLoadBalancer.NUMBER_OF_HOSTS-1
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
    * Creates a list of virtual machines in a Datacenter for a given broker and
    * submit the list to the broker.
    *
    * @param broker The broker that will own the created VMs
    * @return the list of created VMs
    */
  private def createAndSubmitVMs(broker: DatacenterBroker) = {
    val list = new util.ArrayList[NetworkVm]
    var i = 0
    while ( {
      i < RoundRobinLoadBalancer.NUMBER_OF_HOSTS
    }) {
      val vm = createVm(i)
      list.add(vm)

      {
        i += 1; i - 1
      }
    }
    broker.submitVmList(list)
    list
  }

  private def createVm(id: Int) = {
    val vm = new NetworkVm(id, RoundRobinLoadBalancer.HOST_MIPS, RoundRobinLoadBalancer.HOST_PES)
    vm.setRam(RoundRobinLoadBalancer.HOST_RAM).setBw(RoundRobinLoadBalancer.HOST_BW).setSize(RoundRobinLoadBalancer.HOST_STORAGE).setCloudletScheduler(new CloudletSchedulerTimeShared)
    vm
  }

  /**
    * Creates a list of {@link NetworkCloudlet} that together represents the
    * distributed processes of a given fictitious application.
    *
    * @return the list of create NetworkCloudlets
    */
  private def createNetworkCloudlets = {
    val numberOfCloudlets = 2
    val networkCloudletList = new util.ArrayList[NetworkCloudlet](numberOfCloudlets)
    //val selectedVms = randomlySelectVmsForApp(broker, numberOfCloudlets)
    var i = cloudletlistsize
    while ( {
      i < cloudletlistsize + numberOfCloudlets
    }) {
      networkCloudletList.add(createNetworkCloudlet(vmList.get(i)))

      {
        i += 1; i - 1
      }
    }
    cloudletlistsize = cloudletlistsize + numberOfCloudlets

    createTasksForNetworkCloudlets(networkCloudletList)
    networkCloudletList
  }

  private def createTasksForNetworkCloudlets(networkCloudletList: util.ArrayList[NetworkCloudlet]): Unit = {
    import scala.collection.JavaConversions._
    for (cloudlet <- networkCloudletList) {
      RoundRobinLoadBalancer.addExecutionTask(cloudlet)
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

    // broker.submitCloudletList(networkCloudletList)
  }


  /**
    * Creates a {@link NetworkCloudlet}.
    *
    * @param vm the VM that will run the created { @link  NetworkCloudlet)
    * @return
    */
  private def createNetworkCloudlet(vm: NetworkVm) = {
    val netCloudlet = new NetworkCloudlet(4000, RoundRobinLoadBalancer.HOST_PES)
    netCloudlet.setMemory(RoundRobinLoadBalancer.TASK_RAM).setFileSize(RoundRobinLoadBalancer.CLOUDLET_FILE_SIZE).setOutputSize(RoundRobinLoadBalancer.CLOUDLET_OUTPUT_SIZE).setUtilizationModel(new UtilizationModelFull)
    netCloudlet.setVm(vm)
    netCloudlet.setFileSize(RoundRobinLoadBalancer.CLOUDLET_FILE_SIZE)
    netCloudlet.setOutputSize(RoundRobinLoadBalancer.CLOUDLET_OUTPUT_SIZE)
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
    task.setMemory(RoundRobinLoadBalancer.TASK_RAM)
    sourceCloudlet.addTask(task)
    var i = 0
    while ( {
      i < RoundRobinLoadBalancer.NUMBER_OF_PACKETS_TO_SEND
    }) {
      task.addPacket(destinationCloudlet, RoundRobinLoadBalancer.PACKET_DATA_LENGTH_IN_BYTES)

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
    task.setMemory(RoundRobinLoadBalancer.TASK_RAM)
    task.setExpectedPacketsToReceive(RoundRobinLoadBalancer.NUMBER_OF_PACKETS_TO_SEND)
    cloudlet.addTask(task)
  }

  /**
    * Simulates the dynamic arrival of Cloudlets, randomly during simulation runtime.
    * At any time the simulation clock updates, a new Cloudlet will be
    * created with a probability of 30%.
    *
    * @param evt
    */
  private def createRandomCloudlets(evt: EventInfo): Unit = {
    if (random.sample() <= 0.3 && cloudletlistsize<RoundRobinLoadBalancer.NUMBER_OF_HOSTS) {
      printf("\n# Randomly creating 1 Cloudlet at time %.2f\n", evt.getTime)

      broker.submitCloudletList(createNetworkCloudlets)
    }
  }
}
