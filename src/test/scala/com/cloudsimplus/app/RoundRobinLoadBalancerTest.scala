package com.cloudsimplus.app

import java.util

import NetworkAbstract._
import RoundRobinLoadBalancer._
import org.cloudbus.cloudsim.cloudlets.network.NetworkCloudlet
import org.scalatest.Matchers._
import org.scalatest.FunSuite

class RoundRobinLoadBalancerTest extends FunSuite {
  test("Network Cloudlets being created and passed to Round-Robin Load Balancer") {
    val roundRobinLB = new RoundRobinLoadBalancer
    val networkCloudlets = roundRobinLB.createNetworkCloudlets
    networkCloudlets shouldBe a [util.ArrayList[NetworkCloudlet]]
  }
}
