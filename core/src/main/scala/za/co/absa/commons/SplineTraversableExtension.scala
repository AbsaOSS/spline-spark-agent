package za.co.absa.commons

object SplineTraversableExtension {
  implicit class TraversableOps[A <: Traversable[_]](val xs: A) extends AnyVal {

    /**
     * Returns None if Traversable is null or is empty, otherwise returns Some(traversable).
     */
    def asNonEmptyOption: Option[A] = if (xs == null || xs.isEmpty) None else Some(xs)

  }
}
