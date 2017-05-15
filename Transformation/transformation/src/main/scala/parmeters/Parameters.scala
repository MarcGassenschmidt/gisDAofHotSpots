package parmeters

import getisOrd.Weight

/**
  * Created by marc on 12.05.17.
  */
class Parameters {
  var sizeOfRasterLat = 50 //meters
  var sizeOfRasterLon = 50 //meters
  var weightMatrix = Weight.Square
  var weightCols = 5
  var weightRows = 5
  var fromFile = false
  var clusterRange = 50
  var critivalValue = 3.0
  var focal = false
}
