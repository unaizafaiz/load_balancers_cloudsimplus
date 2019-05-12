package com.cloudsimplus.app

import org.cloudbus.cloudsim.allocationpolicies.VmAllocationPolicySimple
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
import org.cloudbus.cloudsim.utilizationmodels.{UtilizationModelDynamic, UtilizationModelFull}
import org.cloudbus.cloudsim.vms.network.NetworkVm
import org.cloudsimplus.builders.tables.CloudletsTableBuilderWithCost
import java.util
import java.util.ArrayList

import com.typesafe.config.{Config, ConfigFactory}
import org.cloudbus.cloudsim.distributions.UniformDistr
import org.cloudbus.cloudsim.utilzationmodels.UtilizationModelHalf
import org.cloudbus.cloudsim.vms.Vm
import org.cloudsimplus.autoscaling.HorizontalVmScalingSimple
import org.cloudsimplus.listeners.EventInfo
import org.slf4j.{Logger, LoggerFactory}


/**
  *Load balancing by horizontal scaling of VM
  */
object HorizontalVmScalingLoadBalancer {

  val logger : Logger = LoggerFactory.getLogger(HorizontalVmScalingLoadBalancer.getClass)

  val defaultConfig: Config = ConfigFactory.parseResources("defaults.conf")

  private val COST_PER_BW: Double = defaultConfig.getDouble("datacenter.cost_per_bw")
  private val COST_PER_SEC: Double = defaultConfig.getDouble("datacenter.cost_per_sec")


  private val NUMBER_OF_VMS = defaultConfig.getInt("vm.number")
  private val VM_PES = defaultConfig.getInt("vm.pes")
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


  private val NUMBER_OF_CLOUDLETS = defaultConfig.getInt("cloudlet.number")
  private val CLOUDLET_PES = defaultConfig.getInt("cloudlet.pes")
  private val CLOUDLET_FILE_SIZE = defaultConfig.getLong("cloudlet.file_size")
  private val CLOUDLET_OUTPUT_SIZE = defaultConfig.getLong("cloudlet.op_size")
  private val PACKET_DATA_LENGTH_IN_BYTES = defaultConfig.getInt("cloudlet.packet_data_length_in_bytes")
  private val NUMBER_OF_PACKETS_TO_SEND = defaultConfig.getInt("cloudlet.number_of_packets")
  private val TASK_RAM = defaultConfig.getInt("cloudlet.task_ram")
  private val CLOUDLET_LENGTHS = Array(2000, 10000, 30000,16000, 4000, 2000, 20000)

  private val DYNAMIC_CLOUDLETS_AT_A_TIME = defaultConfig.getInt("cloudlet.number_of_dynamic_cloudlets")
  private val INITIAL_CLOUDLETS = defaultConfig.getInt("cloudlet.initial_cloudlets")


  private val SCHEDULING_INTERVAL = defaultConfig.getInt("simulation.scheduling_interval")
  private val CLOUDLETS_CREATION_INTERVAL = SCHEDULING_INTERVAL * 2

  /**
    * Starts the execution of the horizontal scaling load balancer simulation
    *
    * @param args command line arguments
    */
  def main(args: Array[String]): Unit = {
    initialise()
  }

  //Variables required for creating ids of VMs and cloudlet
  var cloudletlistsize =0
  var createVms = 0
  var cloudletid = 0

  private val TIME_TO_TERMINATE_SIMULATION: Double = defaultConfig.getInt("simulation.time_to_terminate")


  //Initialising varaibles
  val simulation = new CloudSim()
  private var random = new UniformDistr()
  val datacenter: NetworkDatacenter = createDatacenter
  var broker = new DatacenterBrokerSimple(simulation)
  val vmList = new util.ArrayList[NetworkVm](HorizontalVmScalingLoadBalancer.NUMBER_OF_VMS)
  val cloudletList = new util.ArrayList[NetworkCloudlet]()


  /**
    * Creates, starts, stops the simulation and shows results.
    */
  private def initialise()={
    logger.info("Starting " + getClass.getSimpleName)
    simulation.terminateAt(TIME_TO_TERMINATE_SIMULATION)
    simulation.addOnClockTickListener(createNetworkCloudlets)

    broker.setVmDestructionDelayFunction((vm: Vm) => defaultConfig.getDouble("simulation.vm_destruction_time"))
    vmList.addAll(createListOfScalableVms(HorizontalVmScalingLoadBalancer.NUMBER_OF_VMS))
    createInitialCloudlets()
    broker.submitVmList(vmList)
    broker.submitCloudletList(cloudletList)
    simulation.start
    showSimulationResults()
    showTotalCost()
  }


  /**
    * Get total cost of the simulation
    */
  private def showTotalCost(): Unit = {
    logger.info("Calculate total cost")
    var totalCost = 0.0D
    var meantime = 0.0D
    val newList = broker.getCloudletFinishedList
    cloudletList.forEach(c =>{
      totalCost = totalCost + c.getTotalCost
        meantime = meantime + c.getActualCpuTime
  }
  )
    println("Total cost of execution of " + newList.size + " Cloudlets = $" + Math.round(totalCost * 100D)/100D)
    println("Mean cost of execution of " + newList.size + " Cloudlets = $" + Math.round(totalCost/newList.size()* 100D)/100D)
    println("Total mean time of actualCPUTime for " + newList.size + " Cloudlets = " +Math.round(meantime/cloudletList.size()* 100D)/100D)

  }

  /**
    * Print results of simulation
    */

  private def showSimulationResults(): Unit = {
    logger.info("Print Simulation results")

    val newList = broker.getCloudletFinishedList
    val range = 0 until newList.size()
    new CloudletsTableBuilderWithCost(newList).build()
    println("Number of Actual cloudlets = "+INITIAL_CLOUDLETS+"; dynamic cloudlets = "+(cloudletList.size()-INITIAL_CLOUDLETS))
    import scala.collection.JavaConversions._
    for (host <- datacenter.getHostList[NetworkHost]) {
      if(host.getTotalDataTransferBytes>0)
        println(s"\nHost "+host.getId+" data transferred: "+host.getTotalDataTransferBytes+" bytes")
    }

    println(this.getClass.getSimpleName + " finished!")
  }

  /**
    * Creates the Datacenter
    *
    * @return the Datacenter
    */
  def createDatacenter = {
    logger.info("Creating a datacenter")

    val numberOfHosts = HorizontalVmScalingLoadBalancer.NUMBER_OF_HOSTS * AggregateSwitch.PORTS * RootSwitch.PORTS

    val hostList = new util.ArrayList[Host]
    val range = 0 until numberOfHosts
    range.foreach(_ -> {
      val host = createHost
      hostList.add(host)
    })
    val newDatacenter = new NetworkDatacenter(simulation, hostList, new VmAllocationPolicySimple)
    newDatacenter.setSchedulingInterval(HorizontalVmScalingLoadBalancer.SCHEDULING_INTERVAL)
    newDatacenter.getCharacteristics.setCostPerBw(HorizontalVmScalingLoadBalancer.COST_PER_BW)
    newDatacenter.getCharacteristics.setCostPerSecond(HorizontalVmScalingLoadBalancer.COST_PER_SEC)
    newDatacenter
  }

  /**
    * Create Hosts with configuration parameters
    * @return
    */
  def createHost = {

    logger.info("Creating a new NetworkHost")

    val peList = createPEs(HorizontalVmScalingLoadBalancer.HOST_PES, HorizontalVmScalingLoadBalancer.HOST_MIPS)
    new NetworkHost(HorizontalVmScalingLoadBalancer.HOST_RAM, HorizontalVmScalingLoadBalancer.HOST_BW, HorizontalVmScalingLoadBalancer.HOST_STORAGE, peList).setRamProvisioner(new ResourceProvisionerSimple).setBwProvisioner(new ResourceProvisionerSimple).setVmScheduler(new VmSchedulerTimeShared)
  }

  /**
    * Creates list of PE for Hosts
    * @param numberOfPEs
    * @param mips
    * @return
    */
  def createPEs(numberOfPEs: Int, mips: Long) = {
    logger.info("Creating "+numberOfPEs+" PEs with mips "+mips)

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
  def createNetwork(datacenter: NetworkDatacenter): Unit = {

    logger.info("Creating internal network for "+datacenter)

    val numberOfEdgeSwitches = HorizontalVmScalingLoadBalancer.NUMBER_OF_HOSTS
    val edgeSwitches: ArrayList[EdgeSwitch] = new util.ArrayList[EdgeSwitch]

    (0 until numberOfEdgeSwitches).toArray.foreach(_ => {
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
    * Creates a list of initial VMs in which each VM is able to scale horizontally
    * when it is overloaded.
    *
    * @param numberOfVms number of VMs to create
    * @return the list of scalable VMs
    * @see #createHorizontalVmScaling(Vm)
    */
  def createListOfScalableVms(numberOfVms: Int) = {
    logger.info("Creating  "+numberOfVms+" scalable VMs")

    val newList = new util.ArrayList[NetworkVm](numberOfVms)
    val range = 0 until numberOfVms
    range.foreach{ i =>
      val vm = createVm
      createHorizontalVmScaling(vm)
      newList.add(vm)
    }
    newList
  }

  /**
    * Creates a virtual machine with the given id
    *
    * @return instance of NetworkVM
    */
  private def createVm = {
    val id = createVms

    logger.info("Creating NetworkVM with id "+id)

    createVms+=1
    val vm = new NetworkVm(id, HorizontalVmScalingLoadBalancer.VM_MIPS, HorizontalVmScalingLoadBalancer.VM_PES)
    vm.setRam(HorizontalVmScalingLoadBalancer.VM_RAM).setBw(HorizontalVmScalingLoadBalancer.VM_BW).setSize(HorizontalVmScalingLoadBalancer.VM_STORAGE).setCloudletScheduler(new CloudletSchedulerSpaceShared)
    vm
  }

  /**
    * Creates a {@link HorizontalVmScaling} object for a given VM.
    *
    * @param vm the VM for which the Horizontal Scaling will be created
    * @see #createListOfScalableVms(int)
    */
  private def createHorizontalVmScaling(vm: Vm): Unit = {
    logger.info("Creating a horizontalVMSCaling object for VM "+vm.getId)

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
    * Creates a list of {@link NetworkCloudlet} that together represents the
    * distributed processes of a given fictitious application.
    *
    * @return the list of create NetworkCloudlets
    */
  private def createInitialCloudlets() = {
    logger.info("Creating initial cloudlets")

    val range = 0 until HorizontalVmScalingLoadBalancer.INITIAL_CLOUDLETS
    range.foreach{_=>
      cloudletList.add(createNetworkCloudlet)
    }

    createTasksForNetworkCloudlets(cloudletList)

  }

  private def createTasksForNetworkCloudlets(networkCloudletList: util.ArrayList[NetworkCloudlet]): Unit = {
    logger.info("Creating tasks for network cloudlets")

    import scala.collection.JavaConversions._
    for (cloudlet <- networkCloudletList) {
      addExecutionTask(cloudlet)
    }
  }

  /**
    * Creates a list of {@link NetworkCloudlet} that together represents the
    * distributed processes of a given fictitious application.
    *
    * @return the list of create NetworkCloudlets
    */
  def createNetworkCloudlets(eventInfo: EventInfo): Unit = {
    logger.info("Creating new cloudlet dynamically at runtime")

    //Creating new cloudlets at intervals
    val time = eventInfo.getTime.toLong
    if (time % HorizontalVmScalingLoadBalancer.CLOUDLETS_CREATION_INTERVAL == 0 && time <= TIME_TO_TERMINATE_SIMULATION) {
      val numberOfCloudlets = DYNAMIC_CLOUDLETS_AT_A_TIME
      val networkCloudletList = new util.ArrayList[NetworkCloudlet](numberOfCloudlets)
      val range = 0 until numberOfCloudlets
      range.foreach { _ => {
        if (cloudletList.size() < NUMBER_OF_CLOUDLETS) {
          printf("\t#Creating %d Cloudlets at time %d.\n", numberOfCloudlets, time)
          val cloudlet = createNetworkCloudlet
          cloudletList.add(cloudlet)
          networkCloudletList.add(cloudlet)
        }
      }
      }
      broker.submitCloudletList(networkCloudletList)
      createTasksForNetworkCloudlets(networkCloudletList)
    }
  }



  /**
    * Creates a {@link NetworkCloudlet}.
    *
    * @return
    */
  def createNetworkCloudlet = {
    logger.info("Creating network cloudlet")

    val id = cloudletid
    cloudletid+=1
    val rand = id % CLOUDLET_LENGTHS.size
    val length = CLOUDLET_LENGTHS(rand)
    val netCloudlet = new NetworkCloudlet(id, length, HorizontalVmScalingLoadBalancer.CLOUDLET_PES)
    netCloudlet.setMemory(HorizontalVmScalingLoadBalancer.TASK_RAM).setFileSize(HorizontalVmScalingLoadBalancer.CLOUDLET_FILE_SIZE).setOutputSize(HorizontalVmScalingLoadBalancer.CLOUDLET_OUTPUT_SIZE).setUtilizationModel(new UtilizationModelFull)

    //Using planetLab tracefile to build a CPU Utilization model
    /* val TRACE_FILE = "75-130-96-12_static_oxfr_ma_charter_com_irisaple_wup"

    import org.cloudbus.cloudsim.utilizationmodels.UtilizationModelPlanetLab
    val utilizationCpu = UtilizationModelPlanetLab.getInstance(TRACE_FILE, HorizontalVmScalingLoadBalancer.SCHEDULING_INTERVAL)

    //netCloudlet.setMemory(NetworkTopologyTraceFile.TASK_RAM).setFileSize(NetworkTopologyTraceFile.CLOUDLET_FILE_SIZE).setOutputSize(NetworkTopologyTraceFile.CLOUDLET_OUTPUT_SIZE).setUtilizationModel(new UtilizationModelFull)


    netCloudlet.setMemory(HorizontalVmScalingLoadBalancer.TASK_RAM).setFileSize(HorizontalVmScalingLoadBalancer.CLOUDLET_FILE_SIZE).setOutputSize(HorizontalVmScalingLoadBalancer.CLOUDLET_OUTPUT_SIZE).setUtilizationModelCpu(utilizationCpu)
      .setUtilizationModelBw(new UtilizationModelDynamic(0.2))
      .setUtilizationModelRam(new UtilizationModelDynamic(0.4))*/
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
    task.setMemory(HorizontalVmScalingLoadBalancer.TASK_RAM)
    sourceCloudlet.addTask(task)
    val range = 0 until HorizontalVmScalingLoadBalancer.NUMBER_OF_PACKETS_TO_SEND
   range.foreach(_->
      task.addPacket(destinationCloudlet, HorizontalVmScalingLoadBalancer.PACKET_DATA_LENGTH_IN_BYTES)
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
    logger.info("Adding receiving task from cloudlet "+sourceCloudlet.getId+" to cloudlet "+cloudlet.getId)
    val task = new CloudletReceiveTask(cloudlet.getTasks.size, sourceCloudlet.getVm)
    task.setMemory(HorizontalVmScalingLoadBalancer.TASK_RAM)
    task.setExpectedPacketsToReceive(HorizontalVmScalingLoadBalancer.NUMBER_OF_PACKETS_TO_SEND)
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
}
