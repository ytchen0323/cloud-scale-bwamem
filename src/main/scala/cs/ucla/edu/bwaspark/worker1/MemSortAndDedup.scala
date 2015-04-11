package cs.ucla.edu.bwaspark.worker1

import scala.util.control.Breaks._

import cs.ucla.edu.bwaspark.datatype._

object MemSortAndDedup {
  /**
    *  Sort the MemAlnRegs according to the given order
    *  and remove the redundant MemAlnRegs
    *  
    *  @param regArray alignment registers, which are the output of chain to alignment (after memChainToAln() is applied)
    *  @param maskLevelRedun mask level of redundant alignment registers (from MemOptType object)
    */
  def memSortAndDedup(regArray: MemAlnRegArrayType, maskLevelRedun: Float): MemAlnRegArrayType = {
    if(regArray.curLength <= 1) {
      regArray
    } 
    else {
      //println("before dedup, n: " + regsIn.length)
      //var regs = regArray.regs.sortBy(_.rEnd)
      var regs = regArray.regs.sortBy(r => (r.rEnd, r.rBeg))
/*
      var j = 0
      println("#####")
      regs.foreach(r => {
        print("Reg " + j + "(")
        print(r.rBeg + ", " + r.rEnd + ", " + r.qBeg + ", " + r.qEnd + ", " + r.score + ", " + r.trueScore + ", ")
        println(r.sub + ", "  + r.csub + ", " + r.subNum + ", " + r.width + ", " + r.seedCov + ", " + r.secondary + ")")
        j += 1
        } )
*/      
      var i = 1
      while(i < regs.length) {
        if(regs(i).rBeg < regs(i-1).rEnd) {
          var j = i - 1
          var isBreak = false
          while(j >= 0 && regs(i).rBeg < regs(j).rEnd && !isBreak) {
            // a[j] has been excluded
            if(regs(j).qEnd != regs(j).qBeg) { 
              var oq = 0
              var mr: Long = 0
              var mq = 0
              var or = regs(j).rEnd - regs(i).rBeg // overlap length on the reference
              // overlap length on the query
              if(regs(j).qBeg < regs(i).qBeg) oq = regs(j).qEnd - regs(i).qBeg
              else oq = regs(i).qEnd - regs(j).qBeg
              // min ref len in alignment
              if(regs(j).rEnd - regs(j).rBeg < regs(i).rEnd - regs(i).rBeg) mr = regs(j).rEnd - regs(j).rBeg
              else mr = regs(i).rEnd - regs(i).rBeg
              // min qry len in alignment
              if(regs(j).qEnd - regs(j).qBeg < regs(i).qEnd - regs(i).qBeg) mq = regs(j).qEnd - regs(j).qBeg
              else mq = regs(i).qEnd - regs(i).qBeg
              // one of the hits is redundant
              if(or > maskLevelRedun * mr && oq > maskLevelRedun * mq) {
                if(regs(i).score < regs(j).score) {
                  regs(i).qEnd = regs(i).qBeg
                  // testing
                  //println("(i, j)=(" + i + ", " + j + ") " + or + " " + oq + " " + mr + " " + mq)
                  //println("i: (" + regs(i).qBeg + " " + regs(i).qEnd + " " + regs(i).rBeg + " " + regs(i).rEnd + 
                          //"); j: (" + regs(j).qBeg + " " + regs(j).qEnd + " " + regs(j).rBeg + " " + regs(j).rEnd + ")")
                  isBreak = true
                }
                else {
                  regs(j).qEnd = regs(j).qBeg
                  // testing
                  //println("(i, j)=(" + i + ", " + j + ") " + or + " " + oq + " " + mr + " " + mq)
                  //println("i: (" + regs(i).qBeg + " " + regs(i).qEnd + " " + regs(i).rBeg + " " + regs(i).rEnd + 
                          //"); j: (" + regs(j).qBeg + " " + regs(j).qEnd + " " + regs(j).rBeg + " " + regs(j).rEnd + ")")
                }
              }
            }             
 
            j -= 1
          }

        }

        i += 1
      }

      // exclude identical hits
      regs = regs.filter(r => (r.qEnd > r.qBeg))

/*
      var j = 0
      println("#####")
      regs.foreach(r => {
        print("Reg " + j + "(")
        print(r.rBeg + ", " + r.rEnd + ", " + r.qBeg + ", " + r.qEnd + ", " + r.score + ", " + r.trueScore + ", ")
        println(r.sub + ", "  + r.csub + ", " + r.subNum + ", " + r.width + ", " + r.seedCov + ", " + r.secondary + ")")
        j += 1
        } )
*/      

      regs = regs.sortBy(r => (- r.score, r.rBeg, r.qBeg))
      //println("1st dedup, n: " + regs.length)
      
      i = 1
      while(i < regs.length) {
        if(regs(i).score == regs(i-1).score && regs(i).rBeg == regs(i-1).rBeg && regs(i).qBeg == regs(i-1).qBeg)
          regs(i).qEnd = regs(i).qBeg
        i += 1
      }        

      regs = regs.filter(r => (r.qEnd > r.qBeg))
      //println("2nd dedup, n: " + regs.length)
/*
      j = 0
      regs.foreach(r => {
        print("Reg " + j + "(")
        print(r.rBeg + ", " + r.rEnd + ", " + r.qBeg + ", " + r.qEnd + ", " + r.score + ", " + r.trueScore + ", ")
        println(r.sub + ", "  + r.csub + ", " + r.subNum + ", " + r.width + ", " + r.seedCov + ", " + r.secondary + ")")
        j += 1
        } )
      println
*/    
      regArray.curLength = regs.length 
      regArray.maxLength = regs.length
      regArray.regs = regs 
      regArray
    }
  }
}

