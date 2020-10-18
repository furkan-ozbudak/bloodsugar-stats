val inputFile = sc.textFile("file:///home/cloudera/blood.csv")
val header = inputFile.first()
val data = inputFile.filter(e => e != header)
val tuples = data.map(line => {
  val lines = line.split(',')
  (lines(2), lines(1))
})
val parsedTuples = tuples.map(x => (x._1.toInt, x._2.toDouble)).mapValues(x => (x, 1.0))
val smokers = parsedTuples.filter(x => x._1 == 1)
val nonSmokers = parsedTuples.filter(x => x._1 == 0)
val meanSmoker = smokers.reduceByKey((x,y) => (x._1 + y._1, x._2 + y._2)).map(x => (x._2._1 / x._2._2)).collect()
val meanNonSmoker = nonSmokers.reduceByKey((x,y) => (x._1 + y._1, x._2 + y._2)).map(x => (x._2._1 / x._2._2)).collect()

val smokersC = smokers.collect()
val nonSmokersC = nonSmokers.collect()
var sum = 0.0

smokersC.foreach(x => sum += (meanSmoker(0) - x._2._1) * (meanSmoker(0) - x._2._1))
val smokerVariance = sum / smokersC.length
sum = 0.0

nonSmokersC.foreach(x => sum += (meanNonSmoker(0) - x._2._1) * (meanNonSmoker(0) - x._2._1))
val nonSmokerVariance = sum / nonSmokersC.length
sum = 0.0

println()
println("Category                              Mean                   Variance")
println()
println("Smokers                               " + BigDecimal(meanSmoker(0)).setScale(2, BigDecimal.RoundingMode.HALF_UP)
.toDouble + "                 " + BigDecimal(smokerVariance).setScale(2, BigDecimal.RoundingMode.HALF_UP).toDouble)
println()
println("Non-Smokers                           " + BigDecimal(meanNonSmoker(0)).setScale(2, BigDecimal.RoundingMode.HALF_UP)
.toDouble + "                 " + BigDecimal(nonSmokerVariance).setScale(2, BigDecimal.RoundingMode.HALF_UP).toDouble)
println()

val smokerSample = smokers.sample(false, 0.25)
val nonSmokerSample = nonSmokers.sample(false, 0.25)

var i = 0
var meanSumSmokers = 0.0
var meanSumNonSmokers = 0.0
var varianceSumSmokers = 0.0
var varianceSumNonSmokers = 0.0
var subSum = 0.0

for (i <- 1 to 1000) {
val smokerResample = smokerSample.sample(
true, 1)
val nonSmokerResample = nonSmokerSample.sample(true, 1)
val meanSmokerResample = smokerResample.reduceByKey((x,y) => (x._1 + y._1, x._2 + y._2))
.map(x => (x._2._1 / x._2._2)).collect()
val meanNonSmokerResample = nonSmokerResample.reduceByKey((x,y) => (x._1 + y._1, x._2 + y._2))
.map(x => (x._2._1 / x._2._2)).collect()

val smokerResampleC = smokerResample.collect()
val nonSmokerResampleC = nonSmokerResample.collect()

smokerResampleC.foreach(x => subSum += (meanSmoker(0) - x._2._1) * (meanSmoker(0) - x._2._1))
val varianceSumSmokerResample = subSum / smokersC.length

subSum = 0.0
nonSmokerResampleC.foreach(x => subSum += (meanNonSmoker(0) - x._2._1) * (meanNonSmoker(0) - x._2._1))
val varianceSumNonSmokerResample = subSum / nonSmokersC.length

meanSumSmokers = meanSumSmokers +  meanSmokerResample(0)
meanSumNonSmokers = meanSumNonSmokers + meanNonSmokerResample(0)
varianceSumSmokers = varianceSumSmokers + varianceSumSmokerResample
varianceSumNonSmokers = varianceSumNonSmokers + varianceSumNonSmokerResample
}

var estimatedMeanSmokers = meanSumSmokers / 1000
var estimatedMeanNonSmokers = meanSumNonSmokers / 1000
var estimatedVarianceSmokers = varianceSumSmokers / 1000
var estimatedVarianceNonSmokers = varianceSumNonSmokers / 1000

println()
println("Category                              Mean                   Variance")
println()
println("Smokers                               " + BigDecimal(estimatedMeanSmokers)
.setScale(2, BigDecimal.RoundingMode.HALF_UP).toDouble + "
                 " + BigDecimal(estimatedVarianceSmokers).setScale(2, BigDecimal.RoundingMode.HALF_UP).toDouble)
println()
println("Non-Smokers                           " + BigDecimal(estimatedMeanNonSmokers)
.setScale(2, BigDecimal.RoundingMode.HALF_UP).toDouble + "                 " +
 BigDecimal(estimatedVarianceNonSmokers).setScale(2, BigDecimal.RoundingMode.HALF_UP).toDouble)
