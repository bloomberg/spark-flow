package com.bloomberg.sparkflow.components

/**
  * Created by ngoehausen on 3/3/16.
  */
abstract class Component[IN <: Bundle, OUT <: Bundle](input: IN) {

  def run(): OUT

  lazy val output = run()

}
