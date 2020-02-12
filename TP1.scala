object main extends App{
	// 1) 
	def last(list: List[Int]) : Option[Int] = list match {
	case x::Nil => Some(x)
	case x :: tail => last(tail)
	case _ => None
	}

	println("Last element" + last(List(1, 1, 2, 3, 5, 8)))

	// 2)
	def nth[T](elem: Int, list:List[T]) : Option[T] = (elem,list) match {
		case (0, head :: tail ) => Some(head) 
		case (elem, head :: tail) => nth(elem-1, tail)
		case (_, Nil) => None
	}

	println("i-ème element" + nth(10, List(1, 1, 2, 3, 5, 8)))

	// 3)
	def reverse(list: List[Int]): List[Int] = list match {
		case Nil => Nil
		case x :: Nil => List(x)
		case x :: tail => reverse(tail) ++ List(x)
	}

	println("Reverse " + reverse(List(1, 1, 2, 3, 5, 8)))

	val reverse = List(1, 1, 2, 3, 5, 8)
	println("Reverse foldLeft " + reverse.foldLeft(List[Int]())((acc: List[Int], i) => i :: acc))



	// 4)
	def compress(list: List[Char]): List[Char] = list match {
		case Nil => Nil
		case elem :: Nil => List(elem)
		case elem1 :: elem2 :: tail if elem1 == elem2 => compress(elem2::tail)
		case elem1 :: elem2 :: tail => elem1 :: compress(elem2::tail)
	}

	println("Compress " + compress(List('a', 'a', 'a', 'a', 'b', 'c', 'c', 'a', 'a', 'd', 'e', 'e', 'e', 'e')))

	// 5)
	def encode[T](list: List[T]): List[(Int, T)] = list match {
		case Nil => Nil
		case x :: tail => encode_aux(list,x,0)	
	}

	def encode_aux[T](list: List[T], prev: T, count: Int): List[(Int, T)] = list match {
		case Nil => Nil
		case x :: Nil if x == prev => List((count+1, prev))
		case x :: Nil => List((1,x),(count, prev))
		case x :: tail if x == prev => encode_aux(tail, prev, count+1)
		case x :: tail => (count, prev) :: encode_aux(tail, x, 1)
	}

	println("Encode " + encode(List('a, 'a, 'a, 'a, 'b, 'c, 'c, 'a, 'a, 'd, 'e, 'e, 'e, 'e)))

	// 6)
	def encode_mod[T](seq: List[T]) : List[Any] = {
    /*def flatten_singles(tup: (Int, T)) : Any = tup match {
      case (1, e) => e
      case t => t
    }*/
    encode[T](seq).map(x => if (x._1 == 1) x._2 else x)
  	}

  	println("Encode modified " + encode_mod(List('a, 'a, 'a, 'a, 'b, 'c, 'c, 'a, 'a, 'd, 'e, 'e, 'e, 'e)))

	// petit exo
	val tlist = List(("a", 1), ("b", 4)) // nous voulons juste List("a", "b")
	tlist.map(x => x._1) // tlist.map(_._1)

	// 7)
	def decode(list:List[(Int, String)]): List[String] = list match {
		case Nil => Nil
		case x :: tail => decode_aux(x._1, x._2) ++ decode(tail)
	}

	def decode_aux(n: Int, c:String): List[String] = (n,c) match {
		case (0, _) => Nil
		case (_, c) => c :: decode_aux(n-1, c)
	}

	val list = List((4, "a"), (1, "b"), (2, "c"), (2, "a"), (1, "d"), (4, "e"))

	println("FlatMap Decode " + list.flatMap{t => (0 to t._1-1).map(_ => t._2).toList})

	println("Decode " + decode(List((4, "a"), (1, "b"), (2, "c"), (2, "a"), (1, "d"), (4, "e"))))
}
