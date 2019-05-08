/*
 * CloudSim Plus: A modern, highly-extensible and easier-to-use Framework for
 * Modeling and Simulation of Cloud Computing Infrastructures and Services.
 * http://cloudsimplus.org
 *
 *     Copyright (C) 2015-2018 Universidade da Beira Interior (UBI, Portugal) and
 *     the Instituto Federal de Educação Ciência e Tecnologia do Tocantins (IFTO, Brazil).
 *
 *     This file is part of CloudSim Plus.
 *
 *     CloudSim Plus is free software: you can redistribute it and/or modify
 *     it under the terms of the GNU General Public License as published by
 *     the Free Software Foundation, either version 3 of the License, or
 *     (at your option) any later version.
 *
 *     CloudSim Plus is distributed in the hope that it will be useful,
 *     but WITHOUT ANY WARRANTY; without even the implied warranty of
 *     MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *     GNU General Public License for more details.
 *
 *     You should have received a copy of the GNU General Public License
 *     along with CloudSim Plus. If not, see <http://www.gnu.org/licenses/>.
 */
package org.cloudsimplus.app

import org.cloudbus.cloudsim.allocationpolicies.VmAllocationPolicySimple
import org.cloudbus.cloudsim.brokers.DatacenterBrokerSimple
import org.cloudbus.cloudsim.cloudlets.Cloudlet
import org.cloudbus.cloudsim.core.CloudSim
import org.cloudbus.cloudsim.distributions.UniformDistr
import org.cloudbus.cloudsim.hosts.Host
import org.cloudbus.cloudsim.provisioners.PeProvisionerSimple
import org.cloudbus.cloudsim.provisioners.ResourceProvisionerSimple
import org.cloudbus.cloudsim.resources.Pe
import org.cloudbus.cloudsim.resources.PeSimple
import org.cloudbus.cloudsim.schedulers.cloudlet.CloudletSchedulerTimeShared
import org.cloudbus.cloudsim.schedulers.vm.VmSchedulerTimeShared
import org.cloudbus.cloudsim.utilizationmodels.UtilizationModelFull
import org.cloudbus.cloudsim.vms.Vm
import org.cloudsimplus.autoscaling.HorizontalVmScalingSimple
import org.cloudsimplus.builders.tables.CloudletsTableBuilder
import org.cloudsimplus.listeners.EventInfo
import java.util
import java.util.Comparator.comparingDouble

import org.cloudbus.cloudsim.cloudlets.network.{CloudletExecutionTask, CloudletReceiveTask, CloudletSendTask, CloudletTask, NetworkCloudlet}
import org.cloudbus.cloudsim.datacenters.network.NetworkDatacenter
import org.cloudbus.cloudsim.hosts.network.NetworkHost
import org.cloudbus.cloudsim.network.switches.EdgeSwitch
import org.cloudbus.cloudsim.vms.network.NetworkVm



/**
  * Balancing the load by dynamically creating VMs,
  * according to the arrival of Cloudlets.
  * Cloudlets are {@link #createNewCloudlets(EventInfo) dynamically created and submitted to the broker
 * at specific time intervals}.
  *
  */
object NetworkLB {

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
  val rand = new UniformDistr(0, NetworkLB.CLOUDLET_LENGTHS.length, seed)
  val hostList = new util.ArrayList[Host]//(NetworkLB.HOSTS)
  val vmList = new util.ArrayList[Vm]//(NetworkLB.VMS)
  val cloudletList = new util.ArrayList[NetworkCloudlet]//(NetworkLB.CLOUDLETS)
  val simulation = new CloudSim()
  private val PACKET_DATA_LENGTH_IN_BYTES = 1000
  private val NUMBER_OF_PACKETS_TO_SEND = 1
  private val TASK_RAM = 100

  //var because broker0 is initialised in the initialize function()
  var broker0 = new DatacenterBrokerSimple(simulation)


  /*Enables just some level of log messages.
           Make sure to import org.cloudsimplus.util.Log;*/
  //Log.setLevel(ch.qos.logback.classic.Level.WARN);
  /*You can remove the seed parameter to get a dynamic one, based on current computer time.
          * With a dynamic seed you will get different results at each simulation run.*/
  val seed = 1


  def initialize(): Unit = {
    createDatacenter()
    broker0 = new DatacenterBrokerSimple(simulation)

    /*
           * Defines the Vm Destruction Delay Function as a lambda expression
           * so that the broker will wait 10 seconds before destroying an idle VM.
           * By commenting this line, no down scaling will be performed
           * and idle VMs will be destroyed just after all running Cloudlets
           * are finished and there is no waiting Cloudlet. */

    broker0.setVmDestructionDelayFunction((vm: Vm) => 10.0)
    vmList.addAll(createListOfScalableVms(NetworkLB.VMS))
    createCloudletList()
    broker0.submitVmList(vmList)
    broker0.submitCloudletList(cloudletList)
    simulation.addOnClockTickListener(createNewCloudlets)
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
    val range = 0 until NetworkLB.CLOUDLETS
    range.foreach{_=>
      val cloudlet = createCloudlet
      //cloudlet.setVm(vmList.get(1))
      cloudletList.add(cloudlet)
    }
    createTasksForNetworkCloudlets(cloudletList)

  }

  /**
    * Adds an execution task to the list of tasks of the given
    * {@link NetworkCloudlet}.
    *
    * @param cloudlet the { @link NetworkCloudlet} the task will belong to
    */
  private def addExecutionTask(cloudlet: NetworkCloudlet): Unit = {
    val task = new CloudletExecutionTask(cloudlet.getTasks.size, cloudlet.getLength)
    task.setMemory(TASK_RAM)
    cloudlet.addTask(task)
  }

  /**
    * Adds a send task to the list of tasks of the given {@link NetworkCloudlet}.
    *
    * @param sourceCloudlet the { @link NetworkCloudlet} from which packets will be sent
    * @param destinationCloudlet the destination { @link NetworkCloudlet} to send packets to
    */
  private def addSendTask(sourceCloudlet: NetworkCloudlet, destinationCloudlet: NetworkCloudlet): Unit = {
    val task = new CloudletSendTask(sourceCloudlet.getTasks.size)
    task.setMemory(TASK_RAM)
    sourceCloudlet.addTask(task)
    val range = 0 until NUMBER_OF_PACKETS_TO_SEND
    range.foreach{_=>
      task.addPacket(destinationCloudlet, PACKET_DATA_LENGTH_IN_BYTES)
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
    task.setMemory(TASK_RAM)
    task.setExpectedPacketsToReceive(NUMBER_OF_PACKETS_TO_SEND)
    cloudlet.addTask(task)
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
    if (time % NetworkLB.CLOUDLETS_CREATION_INTERVAL == 0 && time <= 50) {
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

    createTasksForNetworkCloudlets(cloudletList)
  }

  private def createTasksForNetworkCloudlets(networkCloudletList: util.ArrayList[NetworkCloudlet]): Unit = {
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
    * Creates a Datacenter and its Hosts.
    */
  private def createDatacenter(): Unit = {
    val range = 0 until NetworkLB.HOSTS
    range.foreach{ _ =>
      hostList.add(createHost)
    }
    val dc0 = new NetworkDatacenter(simulation, hostList, new VmAllocationPolicySimple)
    dc0.setSchedulingInterval(NetworkLB.SCHEDULING_INTERVAL)
    createNetwork(dc0)
  }

  /**
    * Creates internal Datacenter network.
    *
    * @param datacenter Datacenter where the network will be created
    */
  private def createNetwork(datacenter: NetworkDatacenter): Unit = {
    val edgeSwitches = new java.util.ArrayList[EdgeSwitch]
    var i = 0
    val size = NetworkLB.HOSTS
    for(i<-0 to size){
      print("i="+i)
      val edgeSwitch = new EdgeSwitch(simulation, datacenter)
      edgeSwitches.add(i, edgeSwitch)
      datacenter.addSwitch(edgeSwitch)
    }
    import scala.collection.JavaConversions._
    print("Size of hostlist = "+datacenter.getHostList[NetworkHost].size())
    for (host <- datacenter.getHostList[NetworkHost]) {
      val switchNum = getSwitchIndex(host, edgeSwitches.get(0).getPorts)
      print("switchnum="+switchNum)
      edgeSwitches.get(switchNum).connectHost(host)
    }
  }

  def getSwitchIndex(host: NetworkHost, switchPorts: Int): Int = Math.round(host.getId % Integer.MAX_VALUE / switchPorts)

  private def createHost = {
    val peList = new util.ArrayList[Pe](NetworkLB.HOST_PES)
    val range = 0 until NetworkLB.HOST_PES
    range.foreach{ _ =>
      peList.add(new PeSimple(1000, new PeProvisionerSimple))
    }
    val ram = 2048 // in Megabytes
    val storage = 1000000
    val bw = 10000 //in Megabits/s
    new NetworkHost(ram, bw, storage, peList).setRamProvisioner(new ResourceProvisionerSimple).setBwProvisioner(new ResourceProvisionerSimple).setVmScheduler(new VmSchedulerTimeShared)
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

  private def createVm: Vm = {
    val id = createsVms
    createsVms +=1
    val networkVm = new NetworkVm(id, 1000, 2)
    networkVm.setRam(512).setBw(1000).setSize(10000).setCloudletScheduler(new CloudletSchedulerTimeShared)
    networkVm
  }

  private def createCloudlet = {
    val id = createdCloudlets
    createdCloudlets +=1
    //randomly selects a length for the cloudlet
    val length = NetworkLB.CLOUDLET_LENGTHS(rand.sample.toInt)
    val utilization = new UtilizationModelFull
    val networkCloudlet = new NetworkCloudlet(id, length, 2)
      networkCloudlet.setFileSize(1024).setOutputSize(1024).setUtilizationModel(utilization)
    networkCloudlet
  }
}
