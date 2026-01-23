package omnichain.model

sealed trait EvidenceMode
object EvidenceMode {
  case object IDENTIFIER extends EvidenceMode
  case object BEHAVIORAL extends EvidenceMode

  def fromString(raw: String): EvidenceMode =
    raw.trim.toUpperCase match {
      case "IDENTIFIER" => IDENTIFIER
      case "BEHAVIORAL" => BEHAVIORAL
      case other        =>
        throw new IllegalArgumentException(s"Unsupported evidenceMode: $other")
    }
}
