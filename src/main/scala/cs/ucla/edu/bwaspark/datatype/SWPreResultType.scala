package cs.ucla.edu.bwaspark.datatype

class SWPreResultType(rmax_c: Array[Long],
                      srt_c: Array[SRTType],
		      rseq_c: Array[Byte],
		      rlen_l: Long) {
  var rmax = rmax_c;
  var srt = srt_c;
  var rseq = rseq_c;
  var rlen = rlen_l;
}
