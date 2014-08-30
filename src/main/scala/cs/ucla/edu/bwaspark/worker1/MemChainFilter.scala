package cs.ucla.edu.bwaspark.worker1

import cs.ucla.edu.bwaspark.datatype._
import scala.collection.mutable.MutableList
import scala.math._
import cs.ucla.edu.bwaspark.debug.DebugFlag._

//define a chain wrapper for sorting and filtering
//one wrapper will maintain two chains,
//one main chain
//the other is the longest overlapped chain
class MemChainWrapperType(mainChain_c: MemChainType, //the main chain
                          secondChain_c: MemChainType, //the second order chain
                          beg_i: Int, //the query begin of the first seed
                          end_i: Int, //the query end of the last seed
                          weight_i: Int) { //the weight of the chain, for sorting

  var mainChain = mainChain_c
  var secondChain = secondChain_c
  var beg = beg_i
  var end = end_i
  var weight = weight_i
}

//standalone object for filtering
object MemChainFilter {


  //define function to calculate the weight of a chain
  def memChainWeight(chain: MemChainType): Int = {
    var curQueryEnd = 0
    var weightQuery = 0
    //get the scope of query start->end
    for (i <- 0 until chain.seedsRefArray.length) {
      //if a seed is entirely behind the previous query end
      //1)weight += length of this seed
      //2)current end = the end of this seed
      if (chain.seedsRefArray(i).qBeg >= curQueryEnd) {
        weightQuery += chain.seedsRefArray(i).len
        curQueryEnd = chain.seedsRefArray(i).qBeg + chain.seedsRefArray(i).len
      }
      //if there is overlap, but the seed extends behind the previous query end
      //1)weight += the extended part
      //2)current end = the end of this seed
      else if (chain.seedsRefArray(i).qBeg + chain.seedsRefArray(i).len > curQueryEnd) {
        weightQuery += (chain.seedsRefArray(i).qBeg + chain.seedsRefArray(i).len - curQueryEnd)
        curQueryEnd = chain.seedsRefArray(i).qBeg + chain.seedsRefArray(i).len
      }
      //else, do nothing
    }
    weightQuery

    //There might be a bug in the original code of BWA-MEM
    //we firstly write the code according to its pattern

    var curRefEnd = 0l
    var weightRef = 0
    //get the scope of query start->end
    for (i <- 0 until chain.seedsRefArray.length) {
      //if a seed is entirely behind the previous reference end
      //1)weight += length of this seed
      //2)current end = the end of this seed
      if (chain.seedsRefArray(i).rBeg >= curRefEnd) {
        weightRef += chain.seedsRefArray(i).len
        curRefEnd = chain.seedsRefArray(i).rBeg + chain.seedsRefArray(i).len
      }
      //if there is overlap, but the seed extends behind the previous reference end
      //1)weight += the extended part
      //2)current end = the end of this seed
      else if (chain.seedsRefArray(i).rBeg + chain.seedsRefArray(i).len > curRefEnd) {
        weightRef += (chain.seedsRefArray(i).rBeg + chain.seedsRefArray(i).len - curRefEnd).toInt
        curRefEnd = chain.seedsRefArray(i).rBeg + chain.seedsRefArray(i).len
      }
      //else, do nothing
    }
    if (debugLevel > 0) println("Weight = " + min (weightQuery, weightRef))

    min (weightQuery, weightRef)


  }

  //define filtering function
  //generate new chain array
  def memChainFilter(opt: MemOptType, chains: Array[MemChainType]): Array[MemChainType] = {

    
    if (debugLevel > 0) {
      println("Perform function memChainFilter")
      println("Chains before filtering:")
      println("#chains: " + chains.length)
      chains.map(ele => ele.print())
    }
    
    //if there is less than one chain in chain array
    //we do not need to filter it at all

    if (chains == null) null
    else if (chains.length <= 1) chains

    else {
      
      //first step: create equivalent wrapper array for chain array
      var chainWrapperArray = new Array[MemChainWrapperType](chains.length)

      for (i <- 0 until chainWrapperArray.length) {
        chainWrapperArray(i) = new MemChainWrapperType(chains(i), //the main chain is exactly the corresponding chain in chain array
                                                       null, //the second chain is null
                                                       chains(i).seedsRefArray.head.qBeg, //the query begin of the first seed
                                                       chains(i).seedsRefArray.last.qBeg + chains(i).seedsRefArray.last.len, //the query end of the last seed
                                                       memChainWeight(chains(i))) //the weight calculated by memChainWeight function

      }
      //sort by weight decreasingly
      chainWrapperArray = chainWrapperArray.sortWith( (a, b) => ((a.weight > b.weight) || (a.weight == b.weight && a.mainChain.pos < b.mainChain.pos) ) )
    
      if (debugLevel > 0) {
        println("The first step: sorting by weight")
        println("#chains: " + chainWrapperArray.length)
        chainWrapperArray.map(ele => ele.mainChain.print())
      }
    

      //second step: filtering
      //var wrappersAfterFilter = new MutableList[MemChainWrapperType]()
      var wrappersAfterFilter = new Array[MemChainWrapperType](chainWrapperArray.length)
      var secondaryWrappers = new MutableList[MemChainWrapperType]()
      var idx = 0
      
      //the first chain in the new chain array will automatically be added
      wrappersAfterFilter(idx) = chainWrapperArray(0)
      idx += 1
      //traverse all the other chains, compare them with the chains in the after filter array
      //if there is significant overlap, should be filtered
      for (i <- 1 until chainWrapperArray.length) {

        if (debugLevel > 0) println("The i-loop index for filtering is: " + i)

        //traverse all the chains in the result array
        //if significant overlap found, break
        //we use a boolean flag to mark it
        var isOverlap = false
        var j = 0
        var isInSecondWrapper = false

        while (!isOverlap && j < idx) {
//          if (debugLevel > 0) println("The j-loop index for " + i + "th i-loop filtering is: " + j)
          //judge if there is significant overlap between i and j
//          val beginMax = if (wrappersAfterFilter(j).beg > chainWrapperArray(i).beg) wrappersAfterFilter(j).beg else chainWrapperArray(i).beg
//          val endMin = if (wrappersAfterFilter(j).end > chainWrapperArray(i).end) wrappersAfterFilter(j).end else chainWrapperArray(i).end
          val beginMax = max(wrappersAfterFilter(j).beg, chainWrapperArray(i).beg)
          val endMin = min(wrappersAfterFilter(j).end, chainWrapperArray(i).end)

          //if there is overlap, judge if it has exceeded the threshold
          if (beginMax < endMin) {
            val chainLenJ = wrappersAfterFilter(j).end - wrappersAfterFilter(j).beg
            val chainLenI = chainWrapperArray(i).end - chainWrapperArray(i).beg
            val minLen = min(chainLenI, chainLenJ)
            //if there is significant overlap, mark the flag to true
            if ( (endMin - beginMax) >= minLen * opt.maskLevel ) {
              if (wrappersAfterFilter(j).secondChain == null) {
                wrappersAfterFilter(j).secondChain = chainWrapperArray(i).mainChain
                if (!isInSecondWrapper) {
                  isInSecondWrapper = true;
                }
              }
              if (chainWrapperArray(i).weight < wrappersAfterFilter(j).weight * opt.chainDropRatio && wrappersAfterFilter(j).weight - chainWrapperArray(i).weight >= opt.minSeedLen * 2) isOverlap = true
            }
          }

          j = j + 1

        }
        //if not isOverlap, then add the chain to result array
        if (!isOverlap) {wrappersAfterFilter(idx) = chainWrapperArray(i); idx += 1}
        if (isOverlap && isInSecondWrapper) secondaryWrappers += chainWrapperArray(i)

      }

      //combine the two collections
      var tmpWrapperArray = new Array[MemChainWrapperType](idx + secondaryWrappers.length)
      if (debugLevel > 0) println("The length of the secondaryWrappers is: " + secondaryWrappers.length)
      for (i <- 0 until idx) tmpWrapperArray(i) = wrappersAfterFilter(i)
      for (wrapper <- secondaryWrappers) {
        tmpWrapperArray(idx) = wrapper
        idx += 1
      }
      assert (idx == tmpWrapperArray.length)

      tmpWrapperArray = tmpWrapperArray.sortWith( (a, b) => ((a.weight > b.weight) || (a.weight == b.weight && a.mainChain.pos < b.mainChain.pos) ) )

      //get the new chain array
      var newChainArray: Array[MemChainType] = tmpWrapperArray.map(ele => ele.mainChain).toArray

      //for (i <- 0 until idx) {
      //  //firstly, insert mainChain and secondChain(if available)
      //  newChainList += wrappersAfterFilter(i).mainChain
      //  if (wrappersAfterFilter(i).secondChain != null) newChainList += wrappersAfterFilter(i).secondChain
      //}
      ////Then, remove duplicate
      //val newChainArray = (newChainList.toArray).distinct


/*      for (i <- 0 until idx) {
        //firstly, insert mainChain
        newChainList += wrappersAfterFilter(i).mainChain
      }

      for (i <- 0 until idx) {
        //secondly, insert secondChain if it is avaiable and does not appear before
        if (wrappersAfterFilter(i).secondChain != null && !newChainList.contains(wrappersAfterFilter(i).secondChain)) {
          newChainList += wrappersAfterFilter(i).secondChain
          if (debugLevel > 0) println("Valid secondChain found")
        }
      }
      val newChainArray = newChainList.toArray
*/
    
      if (debugLevel > 0) {
        println("Chains after filtering:")
        println("#chains: " + newChainArray.length)
        newChainArray.map(ele => ele.print())
        println("End function memChainFilter")
      }
    
      //return newChainArray
      newChainArray
    }
  }
}
