package com.cloudsimplus.app

import java.util
import NetworkAbstract._
import RandomLoadBalancer._
import org.cloudbus.cloudsim.cloudlets.network.NetworkCloudlet
import org.scalatest.Matchers._
import org.scalatest.FunSuite

class RandomLoadBalancerTest extends FunSuite {
  test("Network Cloudlets being created and passed to Random Load Balancer") {
    val randomLB = new RandomLoadBalancer
    val networkCloudlets = randomLB.createNetworkCloudlets
    networkCloudlets shouldBe a [util.ArrayList[NetworkCloudlet]]
  }
}
