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
import org.cloudbus.cloudsim.utilizationmodels.{UtilizationModelDynamic, UtilizationModelFull}
import org.cloudbus.cloudsim.vms.network.NetworkVm
import org.cloudsimplus.builders.tables.CloudletsTableBuilder
import java.util
import java.util.ArrayList

import org.cloudbus.cloudsim.distributions.UniformDistr
import org.cloudsimplus.listeners.EventInfo
import java.util.stream.IntStream


/**
  * A simple example simulating a distributed application.
  * It show how 2 {@link NetworkCloudlet}'s communicate,
  * each one running inside VMs on different hosts.
  *
  * @author Manoel Campos da Silva Filho
  */

object NetworkTopologyTraceFile {
  private val NUMBER_OF_HOSTS = 20
  private val HOST_MIPS = 1000
  private val HOST_PES = 4
  private val HOST_RAM = 2048 // host memory (Megabyte)

  private val HOST_STORAGE = 1000000 // host storage

  private val HOST_BW = 10000
  private val CLOUDLET_EXECUTION_TASK_LENGTH = 4000
  private val CLOUDLET_FILE_SIZE = 300
  private val CLOUDLET_OUTPUT_SIZE = 300
  private val PACKET_DATA_LENGTH_IN_BYTES = 1000
  private val NUMBER_OF_PACKETS_TO_SEND = 1
  private val TASK_RAM = 100

  private val TRACE_FILE = "75-130-96-12_static_oxfr_ma_charter_com_irisaple_wup"
  private val SCHEDULING_INTERVAL = 5

  /**
    * Starts the execution of the example.
    *
    * @param args command line arguments
    */
  def main(args: Array[String]): Unit = {
    new NetworkTopologyTraceFile
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


class NetworkTopologyTraceFile private() {

  /**
    * Creates, starts, stops the simulation and shows results.
    */
  println("Starting " + getClass.getSimpleName)
  var cloudletlistsize =0
  val simulation: CloudSim = new CloudSim
  private val TIME_TO_TERMINATE_SIMULATION: Double = 100
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

  private def showSimulationResults(): Unit = {
    val newList = broker.getCloudletFinishedList
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
      i < NetworkTopologyTraceFile.NUMBER_OF_HOSTS
    }) {
      val host = createHost
      hostList.add(host)

      {
        i += 1; i - 1
      }
    }
    val newDatacenter = new NetworkDatacenter(simulation, hostList, new VmAllocationPolicySimple)
    newDatacenter.setSchedulingInterval(5)
    createNetwork(newDatacenter)
    newDatacenter
  }

  private def createHost = {
    val peList = createPEs(NetworkTopologyTraceFile.HOST_PES, NetworkTopologyTraceFile.HOST_MIPS)
    new NetworkHost(NetworkTopologyTraceFile.HOST_RAM, NetworkTopologyTraceFile.HOST_BW, NetworkTopologyTraceFile.HOST_STORAGE, peList).setRamProvisioner(new ResourceProvisionerSimple).setBwProvisioner(new ResourceProvisionerSimple).setVmScheduler(new VmSchedulerTimeShared)
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

    val numberOfEdgeSwitches = NetworkTopologyTraceFile.NUMBER_OF_HOSTS-1
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
      i < NetworkTopologyTraceFile.NUMBER_OF_HOSTS
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
    val vm = new NetworkVm(id, NetworkTopologyTraceFile.HOST_MIPS, NetworkTopologyTraceFile.HOST_PES)
    vm.setRam(NetworkTopologyTraceFile.HOST_RAM).setBw(NetworkTopologyTraceFile.HOST_BW).setSize(NetworkTopologyTraceFile.HOST_STORAGE).setCloudletScheduler(new CloudletSchedulerTimeShared)
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
      i < cloudletlistsize+numberOfCloudlets
    }) {
      networkCloudletList.add(createNetworkCloudlet(vmList.get(i)))

      {
        i += 1; i - 1
      }
    }
    cloudletlistsize = cloudletlistsize + numberOfCloudlets
    //NetworkCloudlet 0 Tasks
    NetworkTopologyTraceFile.addExecutionTask(networkCloudletList.get(0))
    addSendTask(networkCloudletList.get(0), networkCloudletList.get(1))
    //NetworkCloudlet 1 Tasks
    addReceiveTask(networkCloudletList.get(1), networkCloudletList.get(0))
    NetworkTopologyTraceFile.addExecutionTask(networkCloudletList.get(1))
    networkCloudletList
  }


  /**
    * Creates a {@link NetworkCloudlet}.
    *
    * @param vm the VM that will run the created { @link  NetworkCloudlet)
    * @return
    */
  private def createNetworkCloudlet(vm: NetworkVm) = {
    val netCloudlet = new NetworkCloudlet(4000, NetworkTopologyTraceFile.HOST_PES)
    import org.cloudbus.cloudsim.utilizationmodels.UtilizationModel
    import org.cloudbus.cloudsim.utilizationmodels.UtilizationModelPlanetLab
    val utilizationCpu = UtilizationModelPlanetLab.getInstance(NetworkTopologyTraceFile.TRACE_FILE, NetworkTopologyTraceFile.SCHEDULING_INTERVAL)

    //netCloudlet.setMemory(NetworkTopologyTraceFile.TASK_RAM).setFileSize(NetworkTopologyTraceFile.CLOUDLET_FILE_SIZE).setOutputSize(NetworkTopologyTraceFile.CLOUDLET_OUTPUT_SIZE).setUtilizationModel(new UtilizationModelFull)


    netCloudlet.setMemory(NetworkTopologyTraceFile.TASK_RAM).setFileSize(NetworkTopologyTraceFile.CLOUDLET_FILE_SIZE).setOutputSize(NetworkTopologyTraceFile.CLOUDLET_OUTPUT_SIZE).setUtilizationModelCpu(utilizationCpu)
      .setUtilizationModelBw(new UtilizationModelDynamic(0.2))
      .setUtilizationModelRam(new UtilizationModelDynamic(0.4))


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
    val task = new CloudletSendTask(sourceCloudlet.getTasks.size)
    task.setMemory(NetworkTopologyTraceFile.TASK_RAM)
    sourceCloudlet.addTask(task)
    var i = 0
    while ( {
      i < NetworkTopologyTraceFile.NUMBER_OF_PACKETS_TO_SEND
    }) {
      task.addPacket(destinationCloudlet, NetworkTopologyTraceFile.PACKET_DATA_LENGTH_IN_BYTES)

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
    task.setMemory(NetworkTopologyTraceFile.TASK_RAM)
    task.setExpectedPacketsToReceive(NetworkTopologyTraceFile.NUMBER_OF_PACKETS_TO_SEND)
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
    if (random.sample() <= 0.3 && cloudletlistsize<NetworkTopologyTraceFile.NUMBER_OF_HOSTS) {
      printf("\n# Randomly creating 1 Cloudlet at time %.2f\n", evt.getTime)

      broker.submitCloudletList(createNetworkCloudlets)
    }
  }
}
