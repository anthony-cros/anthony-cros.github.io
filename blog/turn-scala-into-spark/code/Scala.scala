package demo.scala

// intentionally verbose so it reads more easily
// shouldn't actually be all in one file either
// ===========================================================================
object Inputs {

	trait HasGene   { val gene:   String } 
	trait HasSample { val sample: String }

	sealed trait F
	  extends HasGene
	     with HasSample

	// ---------------------------------------------------------------------------
	case class F1(
			gene:   String,
			sample: String,
			other1: String)
		extends F

		object F1 {
	
			def apply(line: String): F1 = { // factory of F1s
				val it = line.split("\t").iterator
			
				F1(
					gene   = it.next, // TODO: should add checks
					sample = it.next,
					other1 = it.next)
			}

		}

	// ---------------------------------------------------------------------------
	case class F2(
			gene:    String,
			sample:  String,
			other21: String,
			other22: String)
		extends F

		object F2 {
	
			def apply(line: String): F2 = { // factory of F2s
				val it = line.split("\t").iterator
			
				F2(
					gene    = it.next,
					sample  = it.next,
					other21 = it.next,
					other22 = it.next)
			}

		}

}

// ===========================================================================
object Outputs {

  import Inputs._
  
	// ---------------------------------------------------------------------------
	case class GeneCentric(
			gene: String,
			samples: Iterable[SampleCentric])

	// ---------------------------------------------------------------------------
	case class SampleCentric(
		sample: String,
		extras: Iterable[Extra])

	// ---------------------------------------------------------------------------
	sealed trait Extra
	
		object Extra {

			case class Extra1(
				  other1: String)
				extends Extra

			case class Extra2(
  				other21: String,
  				other22: String)
				extends Extra

			// factory of "extras" (either as Extra1 or Extra2,
			// based on the type of f we get)
			def apply(f: F): Extra =
				// this pattern matching is safe because F is sealed
				// (compiler will warn if we forget a case)
				// pattern matching is one of the most powerful scala constructs,
				// see http://alvinalexander.com/scala/using-match-expression-like-switch-statement
				f match {				
					case F1(_, _, other1) =>           Extra1(other1)
					case F2(_, _, other21, other22) => Extra2(other21, other22)
				}
	
		}

}

// ===========================================================================
object Demo extends App { // i.e. main ("App" trait)

	import Inputs._
	import Outputs._
	import Outputs.Extra._

	// ---------------------------------------------------------------------------
	// These are simply type aliases used for illustration purposes here
	type GenericSeq[A]      = Seq[A]
	type GenericIterable[A] = Iterable[A]

	// ---------------------------------------------------------------------------
	// read lines and transform each into a F1/F2
	// "apply()" is an implicit factory that case classes all have
	val f1s: GenericSeq[F] = readIn(args(0)).map(F1.apply)
	val f2s: GenericSeq[F] = readIn(args(1)).map(F2.apply)
    
	// ---------------------------------------------------------------------------
	val genes: GenericSeq[(String /* genes */, Map[String /* sample */, Iterable[F]])] =
		f1s
		
			// combine both files
			.union(f2s)
			
			// group by "gene" since they both are guaranteed
			// to have this property (they transitively extend HasGene via F)
			.groupBy(f => f.gene)
			
			// ignore key for now and focus on values (the groupings)
			.mapValues(geneGroup =>
				geneGroup
			    
					// within each such grouping,
					// do the same thing but with "sample" 
  					.groupBy(f => f.sample))
            .toSeq

	//---------------------------------------------------------------------------
	val geneCentrics: GenericIterable[GeneCentric] =
		genes
			.map { case (genes, samples) => genes ->
				samples
  					.mapValues(f =>
  					    
  					  	// lastly extract last bits of interest
  					  	f.map(Extra.apply))

  					// we can now build the sample-centric object
  					// (does not capture the parent gene, though it could)
  					// note that this uses a scala trick to be able to pass
  					// a tuple to the primary constructor
  					.map((SampleCentric.apply _).tupled) }

  					// we can now build the gene-centric object
					.map((GeneCentric.apply _).tupled)

	// ---------------------------------------------------------------------------
	// write all as giant json array
	val fw = new java.io.FileWriter("/tmp/demo.json")
		fw.write( // TODO: a scalable solution should stream instead
    			geneCentrics
    				.map(geneCentric =>
    		    			net.liftweb.json.Serialization.writePretty
    		      				(geneCentric)
    		      				(net.liftweb.json.DefaultFormats))
    				.mkString("[", ",", "]"))
    		fw.close()

	println("done.")
	
  // ===========================================================================
  def readIn(filePath: String): GenericSeq[String] =
    scala.io.Source
		  .fromFile(filePath) // TODO: should actually be closed
			.getLines()
			.drop(1) // drop header (TODO: ideally would read schema from it)
			.toSeq // TODO: a scalable solution should stream instead

}
