package com.cloudsimplus.app

import java.util
import java.util.ArrayList

import HorizontalVmScalingLoadBalancer._
import org.cloudbus.cloudsim.cloudlets.network.NetworkCloudlet
import org.cloudbus.cloudsim.core.CloudSim
import org.cloudbus.cloudsim.datacenters.network.NetworkDatacenter
import org.cloudbus.cloudsim.hosts.Host
import org.cloudbus.cloudsim.network.switches.EdgeSwitch
import org.cloudbus.cloudsim.resources.Pe
import org.cloudbus.cloudsim.vms.network.NetworkVm
import org.scalatest.Matchers._
import org.scalatest.FunSuite

class HorizontalVmScalingLoadBalancerTest extends FunSuite {

  private val NUMBER_OF_CLOUDLETS = 500

  test("Datacenter should be created") {
    val testDatacenter = createDatacenter
    testDatacenter shouldBe a [NetworkDatacenter]
  }

  test("Hosts should be created") {
    val testHost = createHost
    testHost shouldBe a [Host]
  }

  test("Created Processig Elements List should be valid PEs") {
    val numberOfPEs = 4
    val mips = 1000
    val testPEList = createPEs(numberOfPEs, mips)
    testPEList shouldBe a [util.ArrayList[Pe]]
    testPEList should have size (numberOfPEs)
  }

  test("Network Topology should be created with specified Edge Switches") {
    val testDatacenter = createDatacenter
    createNetwork(testDatacenter)
  }

  test("Edge Switches should be connected to Hosts in Datacenter") {
    val testDatacenter = createDatacenter
    createNetwork(testDatacenter)

    val numberOfEdgeSwitches = 42
    val edgeSwitches: ArrayList[EdgeSwitch] = new util.ArrayList[EdgeSwitch]

    (0 until numberOfEdgeSwitches).toArray.foreach(_ => {
      val edgeSwitch = new EdgeSwitch(simulation, datacenter)
      edgeSwitches.add(edgeSwitch)
      datacenter.addSwitch(edgeSwitch)
    })

    edgeSwitches should have size (42)
  }

  test("Scalable VMs should be created") {
    val scalableVmList = createListOfScalableVms(750)
    scalableVmList shouldBe a [util.ArrayList[NetworkVm]]
    scalableVmList should have size (750)
  }

  test("Network Cloudlet getting created") {
    val netCloudlet = createNetworkCloudlet
    netCloudlet shouldBe a [NetworkCloudlet]
  }

  test("Network Cloudlets List should be created dynamically at runtime") {
    val simulation = new CloudSim()
    simulation.addOnClockTickListener(createNetworkCloudlets)
    simulation shouldBe a [CloudSim]
    println("Network Cloudlets passed successfully to Simulation on Clock-Tick")
  }

  test("Network Cloudlets List should be scalable") {
    val numberOfDynCloudlets = 1
    val numberOfCloudlets = 500
    val networkCloudletList = new util.ArrayList[NetworkCloudlet](numberOfDynCloudlets)
    val range = 0 until numberOfDynCloudlets
    range.foreach { _ => {
      if (cloudletList.size() < numberOfCloudlets) {
        val cloudlet = createNetworkCloudlet
        cloudletList.add(cloudlet)
        networkCloudletList.add(cloudlet)
      }
    }
    }
    numberOfCloudlets should equal (NUMBER_OF_CLOUDLETS)
    networkCloudletList shouldBe a [util.ArrayList[NetworkCloudlet]]
    networkCloudletList should have size (1)
  }
}
